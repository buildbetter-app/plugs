#!/usr/bin/env python3
"""Reconcile derived OpenRPC pins without rewriting contract declarations."""

import argparse
import hashlib
import json
from pathlib import Path
import re


def unique_members(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON member: " + key)
        value[key] = item
    return value


def digest(data):
    return hashlib.sha256(data).hexdigest()


def replace_once(text, pattern, replacement):
    updated, count = re.subn(pattern, lambda _: replacement, text)
    if count != 1:
        raise ValueError("expected exactly one derived pin: " + pattern)
    return updated


def parameter_semantics(params):
    # Match wire.Param: description is documentation and omitted required is
    # false. Normalize only this comparison, never the raw hashed descriptors.
    normalized = []
    for param in params:
        if set(param) - {"name", "required", "schema", "description"}:
            raise ValueError("unknown parameter member: " + param.get("name", ""))
        required = param.get("required", False)
        if type(required) is not bool:
            raise ValueError("invalid parameter requiredness: " + param["name"])
        if "description" in param and not isinstance(param["description"], str):
            raise ValueError("invalid parameter description: " + param["name"])
        normalized.append({"name": param["name"], "required": required, "schema": param["schema"]})
    return normalized


def reconcile(root, write):
    pending = {}
    descriptors = []
    for relative in ("engdocs/design/plug/contracts/openrpc.json", "internal/api/wire/catalog.json"):
        path = root / relative
        text = path.read_text()
        document = json.loads(text, object_pairs_hook=unique_members)
        registry = document["x-method-registry"]
        entries = registry["methods"]
        names = [entry["name"] for entry in entries]
        methods = {method["name"]: method for method in document["methods"]}
        if len(names) != len(set(names)) or len(names) != registry["methodCount"]:
            raise ValueError("registry names/count mismatch: " + relative)
        if len(document["methods"]) != len(methods) or set(methods) != set(names):
            raise ValueError("method inventory mismatch: " + relative)
        if digest(("\n".join(names) + "\n").encode()) != registry["nameSha256"]:
            raise ValueError("method name pin mismatch: " + relative)
        for entry in entries:
            method = methods[entry["name"]]
            expected = {
                "name": method["name"],
                "params": parameter_semantics(method["params"]),
                "resultSchema": method["result"]["schema"],
                "errors": [error["$ref"] for error in method.get("errors", [])],
            }
            for key, extension in (("effect", "x-effect"), ("permission", "x-permission"), ("idempotency", "x-idempotency"), ("stability", "x-stability"), ("agentCore", "x-agent-core")):
                expected[key] = method[extension]
            actual = {**entry, "params": parameter_semantics(entry["params"])}
            if actual != expected:
                raise ValueError("descriptor parity mismatch: " + entry["name"])
        encoded = json.dumps(entries, ensure_ascii=False, separators=(",", ":")).encode()
        descriptors.append(encoded)
        pending[path] = replace_once(text, r'"descriptorSha256": "[0-9a-f]{64}"', '"descriptorSha256": "' + digest(encoded) + '"')
    if descriptors[0] != descriptors[1]:
        raise ValueError("canonical and embedded descriptors differ")
    catalog = pending[root / "internal/api/wire/catalog.json"].encode()
    test = root / "internal/api/wire/wire_test.go"
    pending[test] = replace_once(test.read_text(), r'if digest\(catalogJSON\) != "[0-9a-f]{64}"', 'if digest(catalogJSON) != "' + digest(catalog) + '"')
    changed = [path for path, text in pending.items() if path.read_text() != text]
    if write:
        for path in changed:
            path.write_text(pending[path])
    elif changed:
        raise ValueError("stale derived pins: " + ", ".join(str(path.relative_to(root)) for path in changed))
    print(json.dumps({"descriptorSha256": digest(descriptors[0]), "catalogSha256": digest(catalog), "updated": [str(path.relative_to(root)) for path in changed]}, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--write", action="store_true", help="write derived pins; default checks without changing files")
    args = parser.parse_args()
    reconcile(args.root.resolve(), args.write)
