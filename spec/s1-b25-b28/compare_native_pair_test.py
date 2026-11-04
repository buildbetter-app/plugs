#!/usr/bin/env python3
"""Synthetic refusal controls; these fixtures provide no native evidence."""

import copy
import base64
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("pair", Path(__file__).with_name("compare_native_pair.py"))
pair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pair)


def fixture(index):
    # Golden vectors independently recomputed from the frozen producer source.
    stamp, identity = (
        ("2026-11-04T08:18:51Z", "016476a8-fcf2-52d1-b22f-638ff5df4b74"),
        ("2026-11-04T08:24:08Z", "de4b4e95-9691-5039-b76b-7dbce0b78f16"),
    )[index]
    comment = dict(zip(("issue_id", "author", "text"), pair.CONTENT))
    comment.update(id=identity, created_at=stamp, schema_version=1)
    projection = {"issues": {"pub-root": {"title": "unchanged", "metadata": {"user": True}}},
                  "edges": [{"from": "pub-root", "to": "pub-dependency", "type": "blocks"}],
                  "comments": [pair.projected_comment(comment)], "memories": {"user": "preserved"}}
    unit = {"comment": comment, "stages": {}, "config": {}, "observations": {},
            "native_comments": {}, "subscriber_comments": {}, "raw_comments": {}}
    raw = {k: v for k, v in comment.items() if k != "schema_version"}
    unit["raw_comments"] = {name: [copy.deepcopy(raw)] for name in pair.RAW_STAGES}
    for store in pair.STORES:
        key, value = "metadata/repo_id", "fresh-" + str(index) + "-" + store
        record = {"kind": "config", "id": key, "project": "",
                  "body": {"key": key, "value": value, "classification": "public"},
                  "source": {"table": "metadata", "fields": {"key": "repo_id", "value": value}}}
        memories = {"user": "preserved", "plug.config/repo_id": value}
        unit["config"][store] = {"config": {key: record}, "user_memory": {"user": "preserved"},
                                 "policy": {"metadata": "public"}, "reverse_memories": memories,
                                 "config_references": [key]}
        unit["observations"][store] = copy.deepcopy({key: record})
        raw = {k: v for k, v in comment.items() if k != "schema_version"}
        unit["native_comments"][store] = [{"kind": "comment", "id": identity, "project": "pub", "body": raw,
                                            "source": {"table": "comments", "fields": copy.deepcopy(raw)}}]
        for stage in pair.STAGES:
            if stage.startswith(store + "-"):
                unit["stages"][stage] = copy.deepcopy(projection)
                if "-plug-" in stage:
                    unit["stages"][stage]["memories"] = copy.deepcopy(memories)
        image = [{"body": comment["text"], "principal": {"kind": "user", "id": comment["author"]},
                  "sourceAuthor": comment["author"], "sourceTime": stamp, "origin": "import", "time": stamp}]
        for engine in pair.ENGINES:
            unit["subscriber_comments"][store + "-" + engine] = [copy.deepcopy(image) for _ in range(16)]
    return unit


class PairComparisonTest(unittest.TestCase):
    def setUp(self):
        self.units = [fixture(0), fixture(1)]

    def refuses(self):
        with self.assertRaises((ValueError, KeyError)):
            pair.compare_pair(self.units)

    def test_distinct_source_derived_ids_are_retained_and_correspond(self):
        before = copy.deepcopy(self.units)
        result = pair.compare_pair(self.units)
        self.assertEqual(result["disposition"], "PASS_BOUNDED_GENERATED_IDENTITY_CORRESPONDENCE")
        self.assertEqual(result["strict57e_pair"], "BLOCK_UNCHANGED")
        self.assertEqual(result["comment_correspondence"][0]["derived_id"], "016476a8-fcf2-52d1-b22f-638ff5df4b74")
        self.assertEqual(result["comment_correspondence"][1]["derived_id"], "de4b4e95-9691-5039-b76b-7dbce0b78f16")
        self.assertEqual(self.units, before)
        self.assertEqual(len(result["literal_differences"]), 3)

    def test_invalid_original_id_or_timestamp_refuses(self):
        for field, value in (("id", "wrong"), ("created_at", "2026-11-04T08:18:51.000Z"),
                             ("created_at", "2026-11-04T08:18:52Z")):
            with self.subTest(field=field, value=value):
                self.setUp()
                self.units[0]["comment"][field] = value
                self.refuses()

    def test_changed_owner_author_or_text_refuses(self):
        for field in ("issue_id", "author", "text"):
            with self.subTest(field=field):
                self.setUp()
                self.units[0]["comment"][field] = "substituted"
                self.refuses()

    def test_missing_extra_duplicate_and_ambiguous_comments_refuse(self):
        for count in (0, 2, 3):
            with self.subTest(count=count):
                self.setUp()
                stage = self.units[0]["stages"][pair.STAGES[0]]
                stage["comments"] *= count
                self.refuses()

    def test_each_within_run_stage_rejects_id_or_content_substitution(self):
        for stage in pair.STAGES:
            for field in ("id", "text"):
                with self.subTest(stage=stage, field=field):
                    self.setUp()
                    self.units[0]["stages"][stage]["comments"][0][field] = "substitution"
                    self.refuses()

    def test_native_source_and_body_substitution_refuse(self):
        for store in pair.STORES:
            for location in ("body", "source"):
                with self.subTest(store=store, location=location):
                    self.setUp()
                    row = self.units[0]["native_comments"][store][0]
                    (row["body"] if location == "body" else row["source"]["fields"])["id"] = "changed"
                    self.refuses()

    def test_every_raw_jsonl_stage_retains_id_timestamp_and_content(self):
        for stage in pair.RAW_STAGES:
            for field in ("id", "created_at", "text"):
                with self.subTest(stage=stage, field=field):
                    self.setUp()
                    self.units[0]["raw_comments"][stage][0][field] = "changed"
                    self.refuses()

    def test_subscriber_content_or_provenance_substitution_refuses(self):
        for field in ("body", "sourceAuthor", "sourceTime"):
            with self.subTest(field=field):
                self.setUp()
                self.units[0]["subscriber_comments"]["source-memory"][15][0][field] = "changed"
                self.refuses()

    def test_real_receipt_shape_has_three_before_seven_after_and_six_root_post_images(self):
        document = {"comments": self.units[0]["subscriber_comments"]["source-memory"][0]}
        snapshot = {pair.CONTENT[0]: document}
        operations = [{"after": copy.deepcopy(snapshot)} for _ in range(7)]
        for index in (0, 2, 4):
            operations[index]["before"] = copy.deepcopy(snapshot)
        posts = []
        for identity in (pair.CONTENT[0], "pub-root.1", pair.CONTENT[0]):
            posts.append({"change": {"Changes": [{"Resource": {"ref": {"id": identity}},
                                                   "Before": document, "After": document}]},
                          "rows": [{"Body": base64.b64encode(json.dumps(document).encode()).decode()}]})
        sub = {"operations": operations, "posts": posts}
        images = pair.subscriber_comment_images(sub)
        self.assertEqual(images, [document["comments"]] * 16)
        for index in (0, 2, 4):
            changed = copy.deepcopy(sub)
            del changed["operations"][index]["before"]
            with self.assertRaisesRegex(ValueError, "before snapshot shape"):
                pair.subscriber_comment_images(changed)
        changed = copy.deepcopy(sub)
        changed["operations"][1]["before"] = snapshot
        with self.assertRaisesRegex(ValueError, "before snapshot shape"):
            pair.subscriber_comment_images(changed)

    def test_issue_edge_and_user_memory_differences_refuse(self):
        for field in ("issues", "edges", "memories"):
            with self.subTest(field=field):
                self.setUp()
                for stage in self.units[1]["stages"].values():
                    stage[field] = {} if field != "edges" else []
                self.refuses()

    def test_config_observation_policy_and_missing_stage_refuse(self):
        self.units[0]["observations"]["source"] = {}
        self.refuses()
        self.setUp()
        self.units[0]["config"]["source"]["policy"]["metadata"] = "secret"
        self.refuses()
        self.setUp()
        del self.units[0]["stages"][pair.STAGES[0]]
        self.refuses()

    def test_duplicate_json_keys_and_wrong_run_binding_refuse(self):
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            pair.decode('{"id":"first","id":"second"}')
        with self.assertRaisesRegex(ValueError, "immutable run identity"):
            pair.load_unit({"source": "wrong"}, 0)

    def test_typed_json_values_and_nonfinite_numbers_do_not_alias(self):
        for first, second in ((True, 1), (False, 0), (1, 1.0), (None, ""), ([], {})):
            with self.subTest(first=first, second=second):
                self.assertFalse(pair.equal({"nested": [first]}, {"nested": [second]}))
        for value in (1, 1.0, True, None):
            self.setUp()
            for stage in self.units[1]["stages"].values():
                stage["issues"]["pub-root"]["metadata"]["user"] = value
            if type(value) is bool:
                pair.compare_pair(self.units)
            else:
                self.refuses()
        for literal in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaisesRegex(ValueError, "nonfinite JSON number"):
                pair.decode(literal)


if __name__ == "__main__":
    unittest.main(verbosity=2)
