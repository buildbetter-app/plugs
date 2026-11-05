#!/usr/bin/env python3
"""Compare two sealed accepted-R418 recordings; never execute a producer.

The input seal binds current product identity and every used byte. This is
bounded content comparison; full-card obligations and independent review remain
separate. The historical nested-pair comparator is unchanged.
"""

import argparse
import base64
import datetime
import hashlib
import io
import json
import re
from pathlib import Path
import tarfile
import uuid


PRODUCER = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
NATIVE_TREE = "930405e863284a3955cbd35e73a6097b606b91a5"
CONTROL = "2d2164ba3d8e456c9aba15fbeb8cae4edc755997"
DESCRIPTOR = "41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
REQUIRED_CHECKS = frozenset({
    "workflow_fixed_ref", "exact_product_identity", "native_descriptor",
    "same_run_preparation", "preparation_helper", "four_equal_exports",
    "unchanged_child_env", "required_bazel_flags", "fixed_ref_guard",
    "exclusive_claim", "current_wrapper_binding", "complete_native_artifacts",
    "official_artifact_zip", "owned_dolt_cleanup",
})
# Frozen producer authorities: comments.go, derivedid.go, rowid.go respectively.
AUTHORITIES = (
    "298d0aaa0c66e5ddf2e1fefeaf4b15d8c99ad008b5828691243d063ef28d7a35",
    "24fbc5d31b6596e7bc8bb97700e83d97ba85094e348dc4cca5e975c9ec5f9c07",
    "d739eea1969675f6a1bfbf5045bee25cce0d9524f9352413c137ba927ff40902",
)
NAMESPACE = uuid.UUID("92da14b5-20a8-5252-8eb2-3839e97b2156")
CONTENT = ("pub-root", "public-recorder", "preserve this public comment")
GENERATED_CONFIG = {
    "metadata/_project_id", "metadata/clone_id",
    "metadata/last_import_time", "metadata/repo_id",
}
STORES = ("source", "target")
ENGINES = ("memory", "sqlite", "postgres")
STAGES = tuple(
    store + "-" + stage for store in STORES for stage in (
        "jsonl-projection", "archive-projection", "reverse-expected-projection",
        "plug-memory-projection", "plug-sqlite-projection",
        "plug-sqlite-reopen-projection", "plug-postgres-projection",
        "plug-postgres-reopen-projection",
    )
)
RAW_STAGES = tuple(store + "-export" for store in STORES) + tuple(
    name.removesuffix("-projection") for name in STAGES if "-plug-" in name
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def decode(data):
    def invalid_constant(value):
        raise ValueError("nonfinite JSON number: " + value)
    return json.loads(data, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def equal(first, second):
    # Python's ordinary equality aliases true, 1 and 1.0. Preserve JSON types
    # recursively, including metadata and user memories, before comparing values.
    if type(first) is not type(second):
        return False
    if isinstance(first, dict):
        return first.keys() == second.keys() and all(equal(value, second[key]) for key, value in first.items())
    if isinstance(first, list):
        return len(first) == len(second) and all(equal(a, b) for a, b in zip(first, second))
    return first == second


def validate_product(product):
    require(type(product) is dict and set(product) == {"commit", "tree", "ordered_parents", "raw_commit_sha256"},
            "complete product identity shape")
    for key, width in (("commit", 40), ("tree", 40), ("raw_commit_sha256", 64)):
        require(type(product[key]) is str and re.fullmatch("[0-9a-f]{" + str(width) + "}", product[key]) is not None,
                "product hash: " + key)
    parents = product["ordered_parents"]
    require(type(parents) is list and len(parents) > 0
            and all(type(p) is str and re.fullmatch(r"[0-9a-f]{40}", p) is not None for p in parents)
            and len(parents) == len(set(parents)), "ordered product parents")


def validate_binding(binding, expected_product=None):
    require(type(binding) is dict, "binding object")
    require(equal(binding["control"], CONTROL) and equal(binding["attempt"], 1),
            "immutable control identity")
    for key in ("run", "artifact"):
        require(type(binding[key]) is str and re.fullmatch(r"[1-9][0-9]*", binding[key]) is not None,
                "decimal string identity: " + key)
    product = binding["product"]
    validate_product(product)
    if expected_product is not None:
        validate_product(expected_product)
        require(equal(product, expected_product), "product identity differs from finite dispatch receipt")


def validate_runtime_summary(summary, binding):
    validate_binding(binding)
    require(type(summary) is dict, "runtime summary object")
    require(equal(summary["product"], binding["product"]) and equal(summary["workflow_head"], CONTROL)
            and equal(summary["run"], binding["run"]) and equal(summary["attempt"], 1)
            and equal(summary["artifact_id"], binding["artifact"]), "separate runtime identity")
    require(equal(summary["literal_exit"], 0) and equal(summary["evidence_status"], "ok")
            and equal(summary["xml_outcomes"], {"PASS": 7}) and summary["raw_xml_match"] is True,
            "separate runtime outcome")
    checks = summary["required_checks"]
    require(type(checks) is dict and set(checks) == REQUIRED_CHECKS
            and all(value is True for value in checks.values()), "exact required-check census and booleans")


def generated_identity(comment):
    require(set(comment) == {"schema_version", "id", "issue_id", "author", "text", "created_at"},
            "unexpected original command comment schema")
    require(equal(comment["schema_version"], 1), "comment command schema version")
    require((comment["issue_id"], comment["author"], comment["text"]) == CONTENT,
            "comment owner/author/text differs from frozen workload")
    stamp = datetime.datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%SZ")
    require(stamp.strftime("%Y-%m-%dT%H:%M:%SZ") == comment["created_at"], "noncanonical timestamp")
    inputs = (*CONTENT, stamp.strftime("%Y-%m-%d %H:%M:%S"))
    encoded = b"".join(b"v" + str(len(value.encode())).encode() + b":" + value.encode() for value in inputs)
    row_digest = digest(encoded)
    identity = str(uuid.uuid5(NAMESPACE, "comments\x1f0\x1f" + row_digest))
    require(comment["id"] == identity, "comment ID does not derive from original live inputs")
    return {"original": comment, "length_prefixed_sha256": row_digest, "derived_id": identity}


def projected_comment(comment):
    return {key: comment[key] for key in ("id", "issue_id", "author", "text")}


def verify_unit(unit):
    comment = unit["comment"]
    identity = generated_identity(comment)
    expected_comment = projected_comment(comment)
    raw_comment = {key: value for key, value in comment.items() if key != "schema_version"}
    require(set(unit["raw_comments"]) == set(RAW_STAGES), "raw JSONL stage census")
    require(all(equal(rows, [raw_comment]) for rows in unit["raw_comments"].values()),
            "original/reverse JSONL full comment identity/time/content")
    require(set(unit["stages"]) == set(STAGES), "missing or extra projection stage")
    for stage, projection in unit["stages"].items():
        require(set(projection) == {"issues", "edges", "comments", "memories"}, "projection shape: " + stage)
        require(equal(projection["comments"], [expected_comment]), "comment identity/content/multiplicity: " + stage)
    for store in STORES:
        stages = unit["stages"]
        ordinary = stages[store + "-jsonl-projection"]
        require(equal(ordinary, stages[store + "-archive-projection"]), "JSONL/archive mismatch")
        expected = stages[store + "-reverse-expected-projection"]
        config = unit["config"][store]
        require(set(config) == {"config", "user_memory", "policy", "reverse_memories", "config_references"},
                "config expectation shape")
        require(equal(config["config"], unit["observations"][store]), "config must equal original independent readbacks")
        require(config["config_references"] == sorted(config["config"]), "config reference census")
        require(all(value == "public" for value in config["policy"].values()), "config policy")
        memories = dict(ordinary["memories"])
        for key, record in config["config"].items():
            table, name = key.split("/", 1)
            value = record["body"]["value"]
            require(equal(record, {
                "kind": "config", "id": key, "project": "",
                "body": {"key": key, "value": value, "classification": "public"},
                "source": {"table": table, "fields": {"key": name, "value": value}},
            }), "config full source/body identity")
            memory_key = "plug.config/" + name
            require(memory_key not in memories, "config/user memory collision")
            memories[memory_key] = value
        require(equal(memories, config["reverse_memories"]), "complete reverse memory expectation")
        for stage in STAGES:
            if stage.startswith(store + "-plug-"):
                actual = stages[stage]
                require(all(equal(actual[key], expected[key]) for key in ("issues", "edges", "comments")),
                        "within-run reverse/reopen preservation: " + stage)
                require(equal(actual["memories"], memories), "within-run reverse memory preservation")
        require(equal(unit["native_comments"][store], [{
            "kind": "comment", "id": comment["id"], "project": "pub", "body": raw_comment,
            "source": {"table": "comments", "fields": raw_comment},
        }]), "native dump comment source/body identity")
    require(set(unit["subscriber_comments"]) == {s + "-" + e for s in STORES for e in ENGINES},
            "subscriber store/engine census")
    for key, images in unit["subscriber_comments"].items():
        require(len(images) == 19, "subscriber before/after/post image census: " + key)
        first = images[0]
        require(len(first) == 1 and set(first[0]) == {
            "body", "principal", "time", "origin", "sourceAuthor", "sourceTime",
        }, "subscriber comment representation")
        value = first[0]
        require(value["body"] == comment["text"] and value["principal"] == {"kind": "user", "id": comment["author"]}
                and value["sourceAuthor"] == comment["author"] and value["sourceTime"] == comment["created_at"]
                and value["origin"] == "import" and bool(value["time"]), "subscriber original content/provenance")
        require(all(equal(image, first) for image in images), "within-run subscriber comment substitution")
    return identity


def compare_pair(units):
    require(len(units) == 2, "exactly two independent units required")
    identities = [verify_unit(unit) for unit in units]
    differences = []
    for store in STORES:
        first, second = [unit["config"][store] for unit in units]
        require(all(equal(first[key], second[key]) for key in ("policy", "config_references", "user_memory")),
                "pair config policy/census/user memory mismatch")
        require(set(first["config"]) == set(second["config"]), "pair config key mismatch")
        allowed_memories = set()
        for key in sorted(first["config"]):
            a, b = first["config"][key], second["config"][key]
            if not equal(a, b):
                require(key in GENERATED_CONFIG, "unexplained pair config difference: " + key)
                values = [a["body"]["value"], b["body"]["value"]]
                require(all(isinstance(value, str) and value for value in values), "generated config value type")
                allowed_memories.add("plug.config/" + key.split("/", 1)[1])
                differences.append({"store": store, "field": key, "values": values,
                                    "cause": "fresh store identity/time, exactly matched own pre-capture CLI observation"})
        for stage in STAGES:
            if not stage.startswith(store + "-"):
                continue
            a, b = [unit["stages"][stage] for unit in units]
            require(equal(a["issues"], b["issues"]) and equal(a["edges"], b["edges"]), "pair issue/edge difference: " + stage)
            require(set(a["memories"]) == set(b["memories"]), "pair memory census: " + stage)
            for key in a["memories"]:
                if not equal(a["memories"][key], b["memories"][key]):
                    require("-plug-" in stage and key in allowed_memories, "unexplained pair memory difference: " + key)
    if not equal(identities[0]["original"], identities[1]["original"]):
        differences.append({"field": "comment.id/created_at", "values": [row["original"] for row in identities],
                            "cause": "exact own live-time inputs and accepted c8 derived identity", "multiplicity": 1})
    return {"disposition": "PASS_BOUNDED_GENERATED_IDENTITY_CORRESPONDENCE", "comment_correspondence": identities,
            "literal_differences": differences, "full_B25_B27": "UNQUALIFIED",
            "scope": "Bounded R418 main archive and replay correspondence only. Native deletion transitions, precedence/type/collision/lease branches and HTTP lifecycle are separate obligations.",
            "NativeAdmission": "ADOPTED_R41800330346_ON_MAIN_7a43c3c6", "FullS1": "0/28", "B26": "SEPARATE", "B05": "PAUSED"}


def subscriber_comment_images(sub):
    require(len(sub["operations"]) == 10 and len(sub["posts"]) == 4, "subscriber workload census")
    refused = sub["operations"][-1]
    require(refused["method"] == "tx.apply" and equal(refused["before"], refused["after"]), "refused full image conservation")
    images = []
    for index, operation in enumerate(sub["operations"][:-1]):
        require(("before" in operation) == (index in (0, 2, 4, 6)), "subscriber before snapshot shape")
        if "before" in operation:
            images.append(operation["before"][CONTENT[0]]["comments"])
        images.append(operation["after"][CONTENT[0]]["comments"])
    for post in sub["posts"]:
        patches = post["change"]["Changes"]
        require(len(patches) == 1, "subscriber patch census")
        patch = patches[0]
        if patch["Resource"]["ref"]["id"] == CONTENT[0]:
            images.extend([patch["Before"]["comments"], patch["After"]["comments"]])
            images.append(decode(base64.b64decode(post["rows"][0]["Body"], validate=True))["comments"])
    return images


def load_unit(binding, index, expected_product=None):
    validate_binding(binding, expected_product)
    assets = binding["assets"]
    used = set()

    def read(name):
        require(name in assets, "missing bound asset: " + name)
        row = assets[name]
        require(type(row["path"]) is str and type(row["bytes"]) is int and row["bytes"] >= 0,
                "asset path and exact integer size")
        path = Path(row["path"])
        require("poll.log" not in path.parts, "excluded collector log")
        data = path.read_bytes()
        require(len(data) == row["bytes"] and digest(data) == row["sha256"], "asset bytes/hash: " + name)
        used.add(name)
        return data

    def obj(name):
        return decode(read(name))

    summary = obj("runtime-summary.json")
    validate_runtime_summary(summary, binding)
    provenance = obj("provenance.json")
    require(equal(provenance["product_source"], binding["product"])
            and provenance["source_commit"] == PRODUCER and provenance["source_tree"] == NATIVE_TREE
            and provenance["recording_id"] == "recording" + str(index + 1), "actual product/native provenance")
    commands = obj("commands.json")
    comment_receipt = [row for row in commands if row["label"] == "source-comment"]
    require(len(comment_receipt) == 1 and equal(comment_receipt[0]["exit"], 0), "original source comment receipt")
    raw = read("source-comment.stdout")
    require(digest(raw) == comment_receipt[0]["stdout_sha256"], "original command return hash")
    unit = {"comment": decode(raw), "stages": {name: obj(name + ".json") for name in STAGES},
            "config": {}, "observations": {}, "native_comments": {}, "subscriber_comments": {}, "raw_comments": {}}
    for name in RAW_STAGES:
        rows = [decode(line) for line in read(name + ".jsonl").splitlines() if line.strip()]
        unit["raw_comments"][name] = [comment for row in rows for comment in row.get("comments", []) or []]
    for store in STORES:
        unit["config"][store] = obj(store + "-config-expectation.json")
        observed = {}
        raw_list = decode(read(store + "-config-readback.stdout"))
        expected_list = {"schema_version": 1}
        for table, suffix in (("config", "config-sql-readback"), ("metadata", "metadata-readback")):
            raw = read(store + "-" + suffix + ".stdout")
            receipt = [row for row in commands if row["label"] == store + "-" + suffix]
            require(len(receipt) == 1 and equal(receipt[0]["exit"], 0) and digest(raw) == receipt[0]["stdout_sha256"],
                    "independent readback receipt")
            parsed = decode(raw)
            require(isinstance(parsed, list), "default native SQL array")
            for row in parsed:
                require(set(row) == {"key", "value"} and all(isinstance(v, str) for v in row.values()), "SQL key/value types")
                key, value = row["key"], row["value"]
                if table == "config" and key.startswith("kv."):
                    continue
                if table == "config" and key != "schema_version":
                    expected_list[key] = value
                identity = table + "/" + key
                require(identity not in observed, "duplicate observed config key")
                observed[identity] = {"kind": "config", "id": identity, "project": "",
                                      "body": {"key": identity, "value": value, "classification": "public"},
                                      "source": {"table": table, "fields": {"key": key, "value": value}}}
        list_receipt = [row for row in commands if row["label"] == store + "-config-readback"]
        require(len(list_receipt) == 1 and equal(list_receipt[0]["exit"], 0)
                and digest(read(store + "-config-readback.stdout")) == list_receipt[0]["stdout_sha256"], "config list receipt")
        require(equal(raw_list, expected_list), "config list/SQL correspondence including schema collision")
        unit["observations"][store] = observed
        with tarfile.open(fileobj=io.BytesIO(read(store + "-plug-dump.tar"))) as archive:
            members = archive.getmembers()
            require(all(member.isfile() for member in members) and len({m.name for m in members}) == len(members),
                    "duplicate or nonfile dump member")
            manifest = decode(archive.extractfile("manifest.json").read())
            require({m.name for m in members} == set(manifest["files"]) | {"manifest.json"}, "dump manifest census")
            for name, row in manifest["files"].items():
                data = archive.extractfile(name).read()
                require(equal(len(data), row["size"]) and digest(data) == row["sha256"], "dump member hash")
            unit["native_comments"][store] = [decode(line) for line in archive.extractfile("records/comment.ndjson").read().splitlines()]
        for engine in ENGINES:
            sub = obj(store + "-plug-" + engine + "-subscriber.json")
            unit["subscriber_comments"][store + "-" + engine] = subscriber_comment_images(sub)
            state = obj(store + "-plug-" + engine + "-post-write-state.json")
            if engine != "memory":
                require(equal(state, obj(store + "-plug-" + engine + "-post-write-reopen-state.json")), "complete post-write SQL reopen state")
    require(used == set(assets), "extra unconsumed asset")
    return unit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--inputs-sha256", required=True)
    args = parser.parse_args()
    raw = args.inputs.read_bytes()
    require(digest(raw) == args.inputs_sha256, "input manifest hash")
    binding = decode(raw)
    require(binding["producer"] == PRODUCER and binding["authority_sha256"] == list(AUTHORITIES), "frozen producer authority")
    consumer = binding.get("consumer")
    validate_product(consumer)
    require(len(binding["recordings"]) == 2, "two own recording units")
    first, second = binding["recordings"]
    validate_binding(first, consumer)
    validate_binding(second, consumer)
    require(equal(first["product"], second["product"]) and first["run"] != second["run"]
            and first["artifact"] != second["artifact"], "same product and distinct runs/artifacts")
    units = [load_unit(row, index, consumer) for index, row in enumerate(binding["recordings"])]
    report = compare_pair(units)
    report["recording_bindings"] = binding
    report["consumer_product"] = consumer
    report["input_manifest_sha256"] = args.inputs_sha256
    report["comparator_sha256"] = digest(Path(__file__).read_bytes())
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
