#!/usr/bin/env python3
"""Adversarial audit-input controls. No native producer or product execution."""
import copy
import unittest

import compare_r418_pair as pair


def fixture():
    binding = {
        "control": pair.CONTROL, "attempt": 1, "run": "12345", "artifact": "67890",
        "product": {"commit": "a" * 40, "tree": "b" * 40,
                    "ordered_parents": ["c" * 40], "raw_commit_sha256": "d" * 64},
    }
    summary = {
        "product": copy.deepcopy(binding["product"]), "workflow_head": pair.CONTROL,
        "run": binding["run"], "attempt": 1, "artifact_id": binding["artifact"],
        "literal_exit": 0, "evidence_status": "ok", "xml_outcomes": {"PASS": 7},
        "raw_xml_match": True, "required_checks": {key: True for key in pair.REQUIRED_CHECKS},
    }
    return binding, summary


class AuditControls(unittest.TestCase):
    def test_accepts_exact_typed_audit(self):
        binding, summary = fixture()
        pair.validate_runtime_summary(summary, binding)

    def test_rejects_outcome_aliases(self):
        for key, values in {
            "literal_exit": [False, 0.0, "0", None, 1],
            "attempt": [True, 1.0, "1", None, 2],
            "xml_outcomes": [{"PASS": 7.0}, {"PASS": True}, {"PASS": "7"}, {}, {"PASS": 7, "SKIP": 0}],
            "raw_xml_match": [1, 1.0, "true", [True], {"ok": True}, False, None],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    binding, summary = fixture()
                    summary[key] = value
                    with self.assertRaises(ValueError):
                        pair.validate_runtime_summary(summary, binding)

    def test_rejects_check_census_and_truthiness(self):
        key = sorted(pair.REQUIRED_CHECKS)[0]
        variants = [{}, {key: True}, dict.fromkeys(pair.REQUIRED_CHECKS, True) | {"extra": True}]
        for value in (1, 1.0, "true", [True], {"ok": True}, False, None):
            variants.append(dict.fromkeys(pair.REQUIRED_CHECKS, True) | {key: value})
        for checks in variants:
            with self.subTest(checks=checks):
                binding, summary = fixture()
                summary["required_checks"] = checks
                with self.assertRaises(ValueError):
                    pair.validate_runtime_summary(summary, binding)

    def test_rejects_identity_aliases_and_incomplete_product(self):
        for key in ("run", "artifact"):
            for value in (12345, True, 12345.0, "", "0", "00123"):
                with self.subTest(key=key, value=value):
                    binding, summary = fixture()
                    binding[key] = value
                    with self.assertRaises(ValueError):
                        pair.validate_runtime_summary(summary, binding)
        for field, value in (("ordered_parents", []), ("ordered_parents", [True]),
                             ("commit", 1), ("tree", "bad"), ("raw_commit_sha256", "x" * 64)):
            binding, summary = fixture()
            binding["product"][field] = value
            summary["product"] = copy.deepcopy(binding["product"])
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                pair.validate_runtime_summary(summary, binding)
        for value in (True, 1.0):
            binding, summary = fixture()
            binding["attempt"] = value
            with self.assertRaises(ValueError):
                pair.validate_runtime_summary(summary, binding)

    def test_rejects_product_substitution_against_finite_dispatch_receipt(self):
        binding, _ = fixture()
        expected = copy.deepcopy(binding["product"])
        for field, value in (
            ("commit", "e" * 40),
            ("tree", "f" * 40),
            ("ordered_parents", ["0" * 40]),
            ("raw_commit_sha256", "e" * 64),
        ):
            with self.subTest(field=field):
                altered = copy.deepcopy(binding)
                altered["product"][field] = value
                with self.assertRaisesRegex(ValueError, "finite dispatch receipt"):
                    pair.validate_binding(altered, expected)

    def test_rejects_runtime_identity_substitution(self):
        for key, value in (("run", "12346"), ("artifact_id", "67891"), ("workflow_head", "e" * 40)):
            binding, summary = fixture()
            summary[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                pair.validate_runtime_summary(summary, binding)


if __name__ == "__main__":
    unittest.main()
