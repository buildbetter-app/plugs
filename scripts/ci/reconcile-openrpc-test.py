#!/usr/bin/env python3
"""Exercise current-contract reconciliation and refusal before any pin writes."""

import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout


spec = importlib.util.spec_from_file_location("reconcile_openrpc", Path(__file__).with_name("reconcile-openrpc.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


class ReconcileOpenRPCTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="reconcile-openrpc-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
        self.document = json.loads((source / "engdocs/design/plug/contracts/openrpc.json").read_text())
        self.paths = [self.root / relative for relative in (
            "engdocs/design/plug/contracts/openrpc.json", "internal/api/wire/catalog.json", "internal/api/wire/wire_test.go")]

    def registry_method(self, document):
        return next(entry for entry in document["x-method-registry"]["methods"] if entry["name"] == "x.workflow.invoke")

    def optional_meta(self, document):
        return next(param for param in self.registry_method(document)["params"] if param["name"] == "_meta")

    def fixture(self, document):
        for path in self.paths:
            path.parent.mkdir(parents=True, exist_ok=True)
        for path in self.paths[:2]:
            path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
        self.paths[2].write_text('if digest(catalogJSON) != "' + "0" * 64 + '" {\n')
        return [path.read_bytes() for path in self.paths]

    def reconcile(self, write):
        with redirect_stdout(io.StringIO()) as output:
            helper.reconcile(self.root, write)
        return json.loads(output.getvalue())

    def assert_regenerates_without_descriptor_changes(self, document):
        self.fixture(document)
        expected = copy.deepcopy(document["x-method-registry"]["methods"])
        raw_hash = hashlib.sha256(json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        result = self.reconcile(True)
        self.assertEqual(result["descriptorSha256"], raw_hash)
        for path in self.paths[:2]:
            after = json.loads(path.read_text())
            self.assertEqual(after["x-method-registry"]["methods"], expected)
            self.assertEqual(after["methods"], document["methods"])
            self.assertEqual(after["components"], document["components"])
        generated = [path.read_bytes() for path in self.paths]
        self.assertEqual(self.reconcile(False)["updated"], [])
        self.assertEqual([path.read_bytes() for path in self.paths], generated)

    def test_current_b26_optional_metadata_preserves_raw_hash_and_contract(self):
        meta = self.optional_meta(self.document)
        self.assertIn("description", meta)
        self.assertNotIn("required", meta)
        self.assert_regenerates_without_descriptor_changes(self.document)

    def test_explicit_optional_and_different_description_preserve_semantics(self):
        meta = self.optional_meta(self.document)
        meta["required"] = False
        meta["description"] = "Documentation differs; schema and optionality remain identical."
        self.assert_regenerates_without_descriptor_changes(self.document)

    def test_parameter_name_schema_and_requiredness_mismatches_refuse_without_writes(self):
        for field, value in (("name", "other_meta"), ("schema", {"type": "integer"}), ("required", True)):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.document)
                self.optional_meta(changed)[field] = value
                before = self.fixture(changed)
                with self.assertRaisesRegex(ValueError, "descriptor parity mismatch: x.workflow.invoke"):
                    self.reconcile(True)
                self.assertEqual([path.read_bytes() for path in self.paths], before)

    def test_unknown_parameter_member_and_invalid_types_refuse_without_writes(self):
        for field, value, message in (("default", {}, "unknown parameter member"),
                                       ("required", 0, "invalid parameter requiredness"),
                                       ("required", "false", "invalid parameter requiredness"),
                                       ("description", {}, "invalid parameter description")):
            with self.subTest(field=field, value=value):
                changed = copy.deepcopy(self.document)
                self.optional_meta(changed)[field] = value
                before = self.fixture(changed)
                with self.assertRaisesRegex(ValueError, message):
                    self.reconcile(True)
                self.assertEqual([path.read_bytes() for path in self.paths], before)

    def test_unknown_canonical_parameter_member_refuses(self):
        method = next(method for method in self.document["methods"] if method["name"] == "x.workflow.invoke")
        method["params"][-1]["default"] = {}
        before = self.fixture(self.document)
        with self.assertRaisesRegex(ValueError, "unknown parameter member"):
            self.reconcile(True)
        self.assertEqual([path.read_bytes() for path in self.paths], before)

    def test_other_descriptor_mismatches_still_refuse(self):
        for field, value in (("permission", "read"), ("resultSchema", {"type": "integer"}), ("unknownSemanticMember", True)):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.document)
                self.registry_method(changed)[field] = value
                before = self.fixture(changed)
                with self.assertRaisesRegex(ValueError, "descriptor parity mismatch: x.workflow.invoke"):
                    self.reconcile(True)
                self.assertEqual([path.read_bytes() for path in self.paths], before)


if __name__ == "__main__":
    unittest.main()
