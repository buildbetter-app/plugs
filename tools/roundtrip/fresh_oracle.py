"""Write one source-bound oracle from the reviewed independent row replay.

This adapter does not import receiver code, invoke Plug or reuse another case's
rows. The four reference modules are verified before import. All outputs are
exclusive creations beneath this invocation's new directory.
"""
import argparse
import datetime
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tarfile


def guard():
    if any(os.path.lexists(p) for p in ("/tmp/plug-swarm/PAUSE", "/tmp/plug-swarm/STOP")):
        raise RuntimeError("public oracle stopped")


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path, chunks):
    guard()
    with path.open("xb") as stream:
        for chunk in chunks:
            guard()
            stream.write(chunk)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o444)
    return {"path": str(path), "sha256": sha(path), "bytes": path.stat().st_size}


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--helpers", required=True, type=Path)
    parser.add_argument("--helpers-sha256", required=True)
    parser.add_argument("--restore-time", required=True)
    parser.add_argument("--producer-head", required=True)
    args = parser.parse_args()
    guard()
    for path in (args.capture, args.helpers):
        if not path.is_absolute() or path.resolve(strict=True) != path:
            raise ValueError("canonical absolute source/helper paths required")
    if not args.output.is_absolute() or args.output.exists():
        raise ValueError("absent absolute owned output required")
    if sha(args.helpers) != args.helpers_sha256:
        raise ValueError("immutable independent replay modules differ")
    args.output.mkdir(mode=0o700)
    helper_dir = args.output / "helpers"
    helper_dir.mkdir(mode=0o700)
    with tarfile.open(args.helpers, "r:gz") as archive:
        members = archive.getmembers()
        expected = {"native_delta_wire_v2.py", "native_delta_row_policy_v2.py",
                    "native_delta_replay_v2.py", "native_delta_semantic_policy_v2.py"}
        if len(members) != 4 or {m.name for m in members} != expected:
            raise ValueError("independent module census differs")
        for member in members:
            if not member.isfile() or member.size > 1 << 20:
                raise ValueError("nonregular independent module")
            write(helper_dir / member.name, [archive.extractfile(member).read()])
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(helper_dir))
    wire = importlib.import_module("native_delta_wire_v2")
    rows = importlib.import_module("native_delta_row_policy_v2")
    semantic = importlib.import_module("native_delta_semantic_policy_v2")
    root_path = args.capture / "capture-set.json"
    root = wire.strict_json(root_path.read_bytes())
    if len(root["projects"]) != 1:
        raise ValueError("one actual native project required")
    root_sha = sha(root_path)
    oracle = args.output / "oracle"
    oracle.mkdir(mode=0o700)
    scratch = args.output / "scratch"
    scratch.mkdir(mode=0o700)
    ledger, states = [], []
    restore_time = datetime.datetime.fromisoformat(args.restore_time.replace("Z", "+00:00"))
    with wire.CaptureSet(str(args.capture), expected_store_id=root["store_id"],
                         expected_projects=root["projects"], expected_head=root["head"],
                         guard=guard) as capture:
        with semantic.SemanticReplay(str(scratch), guard=guard) as replay:
            replay_result = replay.consume(capture)
            for state_id, kind, schema_id, physical_root in replay._iter(
                    "SELECT id,kind,schema_id,root FROM states ORDER BY id"):
                guard()
                sequence, commit_id = 0, replay.header["head"]
                if kind == "commit":
                    commit_id, encoded = replay._one(
                        "SELECT id,sequence FROM commits WHERE state_id=?", (state_id,))
                    sequence = int.from_bytes(encoded, "big")
                table_count = 0
                for name_raw, encoded_count, table_schema_id, physical_sha in replay._iter(
                        "SELECT name,rows,table_schema_id,sha256 FROM state_tables WHERE state_id=? ORDER BY name",
                        (state_id,)):
                    table_count += 1
                    name = name_raw.decode()
                    definition = replay._definition(schema_id, name)
                    history = "retained-native-commit"
                    if kind == "current":
                        classification = replay._one(
                            "SELECT history FROM policy_classification WHERE name=?", (name_raw,))
                        history = classification[0] if classification else "opaque-native-physical"
                    imported = 0

                    def body():
                        nonlocal imported
                        yield b'{"source_identity":{"store_id":' + rows._go_string(replay.header["store_id"])
                        yield b',"project":' + rows._go_string(replay.header["projects"][0]) + b'}'
                        yield b',"table":' + rows._go_string(name) + b',"history":' + rows._go_string(history)
                        yield b',"columns":'
                        yield from rows._go_json(sorted(c["name"] for c in definition["columns"]), guard)
                        yield b',"rows":['
                        for (normalized,) in replay._iter(
                                "SELECT normalized FROM state_rows WHERE state_id=? AND name=? ORDER BY normalized,key",
                                (state_id, name_raw)):
                            if kind == "current" and name == "leases":
                                lease = rows._decode(normalized, guard)
                                expiry = datetime.datetime.fromisoformat(
                                    rows._utc_time(lease["lease_expires_at"]).replace("Z", "+00:00"))
                                if expiry <= restore_time:
                                    continue
                            if not normalized.endswith(b"\n"):
                                raise ValueError("source normalized envelope framing differs")
                            if imported:
                                yield b','
                            yield normalized[:-1]
                            imported += 1
                        yield b']}'

                    filename = f"{kind}-{sequence:020d}-{name}.table.json"
                    binding = write(oracle / filename, body())
                    ledger.append(dict(kind=kind, sequence=sequence, commit_id=commit_id,
                                       family=name, history=history, file=filename,
                                       sha256=binding["sha256"], bytes=binding["bytes"],
                                       source_rows=int.from_bytes(encoded_count, "big"), imported_rows=imported,
                                       schema_id=table_schema_id, physical_sha256=physical_sha,
                                       config_policy=replay.header["config_policy"]))
                states.append(dict(kind=kind, sequence=sequence, commit_id=commit_id,
                                   table_count=table_count, physical_root_sha256=physical_root))
            if not capture.transport_verified or not replay.policy_verified or capture.root_sha256 != root_sha:
                raise ValueError("independent transport/semantic/root authentication incomplete")
            commits = replay.header["commit_count"]
            if len(states) != commits + 2 or sorted(s["sequence"] for s in states if s["kind"] == "commit") != list(range(1, commits + 1)):
                raise ValueError("complete state census differs, including empty source commits")
            write(oracle / "independent-replay.json", [json_bytes(replay_result)])
    ledger_binding = write(oracle / "tables.ndjson", (json_bytes(item) for item in ledger))
    states_binding = write(oracle / "states.ndjson", (json_bytes(item) for item in states))
    request = dict(format="public-native-capture-proof/1", capture=str(args.capture),
                   capture_sha256=root_sha,
                   source_identity=dict(store_id=root["store_id"], project=root["projects"][0]),
                   producer_head=args.producer_head, oracle=str(oracle),
                   ledger_sha256=ledger_binding["sha256"], states_sha256=states_binding["sha256"],
                   restore_time=args.restore_time, restore_mode="full",
                   journal_table="bd_events_journal", commits=commits)
    binding = write(args.output / "request.json", [json_bytes(request)])
    write(args.output / "INPUT-BINDING.json", [json_bytes(dict(
        format="public-id-style-a-independent-oracle/1", request=binding,
        capture_sha256=root_sha, helpers_sha256=args.helpers_sha256,
        adapter_sha256=sha(Path(__file__)), ledger=ledger_binding, states=states_binding,
        receiver_execution="NOT_RUN", equality_credit=0))])
    print(json.dumps(binding, sort_keys=True))


if __name__ == "__main__":
    main()
