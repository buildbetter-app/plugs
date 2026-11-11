#!/usr/bin/env python3
"""Generate the adopted additive blocked contract using the existing tools."""

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


source = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
output = Path(os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]) / "b02-generated"
spec = importlib.util.spec_from_file_location("reconcile", source / "scripts/ci/reconcile-openrpc.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
paths = ("engdocs/design/plug/contracts/openrpc.json", "internal/api/wire/catalog.json", "internal/api/wire/wire_test.go")
with tempfile.TemporaryDirectory(prefix="b02-contract-") as temporary:
    root = Path(temporary)
    for relative in paths:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((source / relative).read_bytes())
    for relative in paths[:2]:
        path = root / relative
        document = json.loads(path.read_text(), object_pairs_hook=helper.unique_members)
        ready = copy.deepcopy(next(method for method in document["methods"] if method["name"] == "bead.ready"))
        ready["name"] = "bead.blocked"
        ready["summary"] = "List graph-blocked beads and readable reasons in subject ID order."
        ready["description"] = "Read-only live mixed graph projection. Stored blocked flags are ignored. Subject permissions and filters precede pagination. Hidden and unresolved reasons contribute counts only. Native membership and displayed reasons are evaluated separately; adopted category and outcome semantics apply. Closed and literally pinned subjects are excluded."
        ready["params"] = [param for param in ready["params"] if param["name"] != "viewId"]
        ready["result"]["schema"] = {"$ref": "#/components/schemas/BlockedPage"}
        if not any(method["name"] == ready["name"] for method in document["methods"]):
            document["methods"].append(ready)
        schemas = document["components"]["schemas"]
        schemas["BlockedBead"] = {"type": "object", "additionalProperties": False, "required": ["bead", "blockedBy", "blockedByCount", "hiddenBlockers"], "properties": {
            "bead": {"$ref": "#/components/schemas/Bead"},
            "blockedBy": {"type": "array", "items": {"$ref": "#/components/schemas/Ref"}},
            "blockedByCount": {"type": "integer", "minimum": 0},
            "hiddenBlockers": {"type": "integer", "minimum": 0}}}
        schemas["BlockedPage"] = copy.deepcopy(schemas["BeadPage"])
        schemas["BlockedPage"]["properties"]["items"]["items"] = {"$ref": "#/components/schemas/BlockedBead"}
        entry = copy.deepcopy(next(entry for entry in document["x-method-registry"]["methods"] if entry["name"] == "bead.ready"))
        entry["name"], entry["resultSchema"] = ready["name"], ready["result"]["schema"]
        entry["params"] = [param for param in entry["params"] if param["name"] != "viewId"]
        registry = document["x-method-registry"]
        if not any(item["name"] == entry["name"] for item in registry["methods"]):
            registry["methods"].append(entry)
        registry["methods"].sort(key=lambda item: item["name"])
        registry["methodCount"] = len(registry["methods"])
        registry["nameSha256"] = hashlib.sha256(("\n".join(item["name"] for item in registry["methods"]) + "\n").encode()).hexdigest()
        path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
    test = root / paths[2]
    text = test.read_text().replace("got != 86", "got != 87").replace("len(document.Methods) != 86", "len(document.Methods) != 87")
    text = text.replace('`"methodCount": 86`, `"methodCount": 85`', '`"methodCount": 87`, `"methodCount": 86`')
    text = text.replace('`"nameSha256": "bedf3c90`', '`"nameSha256": "' + registry["nameSha256"][:8] + '`')
    test.write_text(text)
    helper.reconcile(root, True)
    bootstrap = root / "internal/server/rpc/openrpc.json"
    bootstrap.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(Path(os.environ["TEST_SRCDIR"]) / sys.argv[1]), "-catalog", str(root / paths[1]), "-out", str(bootstrap)], check=True)
    for relative in (*paths, "internal/server/rpc/openrpc.json"):
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((root / relative).read_bytes())
    print("B02 adopted contract generated; review and apply the retained source artifacts")
