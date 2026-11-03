#!/usr/bin/env python3
"""SYNTHETIC evaluator tests. These records are never actual nightly receipts.

Native REST/ZIP/report shapes are sanitized; the source Git repository is a
throwaway fixture, not Plug history. No network, workload, or date waiting.
"""

import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile


spec = importlib.util.spec_from_file_location("nightly_streak", Path(__file__).with_name("verify.py"))
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def encoded(value):
    return (json.dumps(value, indent=2) + "\n").encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


class SyntheticReceiptTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="SYNTHETIC-n1-evaluator-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repository = self.root / "synthetic-source"
        self.repository.mkdir()
        # Real Git object resolution, with no real project history or credentials.
        self.git("init", "--quiet")
        self.fixtures, self.inventories = {}, {}
        for name, label in (("internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar", "source"),
                            ("internal/integration/offlinehistory/testdata/full.tar", "full"),
                            ("internal/integration/offlinehistory/testdata/partial.tar", "partial")):
            members = {"manifest.json": encoded({"synthetic_evaluator_fixture": label}),
                       "records/bead.ndjson": b'{"id":"demo-a","synthetic":true}\n',
                       "config/sources.json": encoded({"synthetic": label})}
            if label != "source":
                members["history/source.json"] = encoded({"synthetic": label})
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w:") as archive:
                for member_name, body in members.items():
                    info = tarfile.TarInfo(member_name)
                    info.size = len(body)
                    archive.addfile(info, io.BytesIO(body))
            body = output.getvalue()
            self.fixtures[name] = body
            self.inventories[digest(body)] = {key: digest(value) for key, value in members.items()}
        for name, body in {**self.fixtures, ".github/workflows/plug-nightly-restore-diffs.yml": b"# SYNTHETIC source fixture\n"}.items():
            path = self.repository / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        self.git("add", ".")
        self.git("-c", "user.name=Synthetic Evaluator", "-c", "user.email=synthetic@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "SYNTHETIC evaluator fixture only")
        self.head = self.git("rev-parse", "HEAD").decode().strip()
        self.tree = self.git("rev-parse", "HEAD^{tree}").decode().strip()
        self.receipts = [self.make_receipt(index, f"2026-02-{27 + index:02d}") for index in range(2)]
        self.receipts.append(self.make_receipt(2, "2026-03-01"))  # month boundary
        self.originals = {path: path.read_bytes() for directory in self.receipts for path in directory.iterdir()}

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repository), *args], stderr=subprocess.PIPE)

    def make_receipt(self, index, date):
        directory = self.root / f"SYNTHETIC-receipt-{index}"
        directory.mkdir()
        run_id = 90000000001 + index
        repo = {"id": 900000000, "full_name": "buildbetter-app/plugs"}
        run = {
            "id": run_id, "workflow_id": 9000000, "run_attempt": 1,
            "path": ".github/workflows/plug-nightly-restore-diffs.yml", "event": "schedule",
            "status": "completed", "conclusion": "success", "head_branch": "main", "head_sha": self.head,
            "created_at": date + "T04:28:43Z", "run_started_at": date + "T04:28:44Z", "updated_at": date + "T04:33:27Z",
            "repository": repo, "head_repository": repo, "synthetic_evaluator_fixture": True,
            "display_title": "SYNTHETIC evaluator test - no actual nightly evidence",
        }
        checkout = {"schema": "plug-nightly-restore-run/v1", "expected_sha": self.head, "head": self.head,
                    "tree": self.tree, "checkout_verified": True, "run_id": str(run_id), "run_attempt": "1", "event": "schedule",
                    "fixture_scope": "checked-in public synthetic snapshot and history fixtures only",
                    "night_streak": "not evaluated by a single run"}
        names = ["TestNightlyFreshStoreDiffs", "TestNightlyHistoryFreshStoreDiffs", "TestNightlyHistoryRefusalsNoWrites"]
        snapshots, history, refusals = [], [], []
        cases = ["strict-default-full", "explicit-full", "explicit-partial", "snapshot-full", "snapshot-partial"]
        controls = [("default-refuses-partial", "non-contiguous"), ("full-refuses-partial", "non-contiguous"),
                    ("missing-binding", "unmappable"), ("changed-digest", "unmappable"), ("changed-time", "unmappable")]
        source_hash, full_hash, partial_hash = [digest(body) for body in self.fixtures.values()]
        for driver in ("memory", "sqlite", "postgres"):
            for ordinal in (1, 2):
                names.append(f"TestNightlyFreshStoreDiffs/{driver}/fresh-{ordinal}")
                snapshots.append({"driver": driver, "fresh_restore": ordinal, "status": "pass",
                                  "incarnation": f"snapshot-{driver}-{ordinal}",
                                  "census": self.census("a" * 64, True, self.inventories[source_hash])})
                for case in cases:
                    names.append(f"TestNightlyHistoryFreshStoreDiffs/{case}/{driver}/fresh-{ordinal}")
                    head, floor = (1, 1) if case.startswith("snapshot-") else ((3, 1) if case == "explicit-partial" else (5, 0))
                    archive_hash = partial_hash if case.endswith("partial") else full_hash
                    current = self.census(archive_hash)
                    images = {str(i): {"demo-a": f"{i:064x}"} for i in range(1, head)}
                    images[str(head)] = current["record_sha256"]
                    versions = {"demo-a": [{"revision": i, "base": i == 1, "deleted": False,
                                             "image_sha256": images[str(i)]["demo-a"]} for i in range(1, head + 1)]}
                    for identity, image in current["record_sha256"].items():
                        if identity != "demo-a":
                            versions[identity] = [{"revision": head, "base": True, "deleted": False, "image_sha256": image}]
                    entries = []
                    for revision in range(1, head + 1):
                        changes = [{"ref": {"id": "demo-a", "branch": 0}, "op": "create" if revision == 1 else "patch",
                                    "fromRevision": revision - 1, "changedPaths": ["/" if head == 1 else ""]}]
                        if revision == head:
                            changes.extend({"ref": {"id": identity, "branch": 0}, "op": "create", "fromRevision": 0,
                                            "changedPaths": ["/" if head == 1 else ""]} for identity in current["ids"] if identity != "demo-a")
                        entry = {"revision": revision, "branch": 0, "incarnation": "", "kind": "restore",
                                 "time": "0001-01-01T00:00:00Z", "principal": {"kind": "user", "id": "n1-test-restore-admin"},
                                 "chain": [], "changes": changes}
                        if revision == head:
                            entry["idempotencyKey"] = "b" * 32
                        entries.append(entry)
                    census = {"head": head, "floor": floor, "current": current, "changesets_unavailable_through": floor,
                              "as_of_record_sha256": images, "versions": versions,
                              "changesets_after_floor": entries[floor:], "restore_receipt": entries[-1]}
                    if not case.startswith("snapshot-"):
                        sequences = [(4, 2)] if floor else [(i, i) for i in range(1, 5)]
                        census["history_evidence"] = {"source": "beads-dolt-images/1", "mode": "partial" if floor else "full",
                            "complete": not bool(floor), "first": 4 if floor else 1, "head": 4, "floor": floor,
                            "gaps": [{"first": 1, "last": 3, "reason": "not-retained"}] if floor else [],
                            "mapping": [{"sequence": sequence, "revision": revision, "eventIds": [str(sequence)],
                                         "commitIds": [f"{sequence:032x}"]} for sequence, revision in sequences]}
                    history.append({"case": case, "driver": driver, "fresh_restore": ordinal,
                                    "incarnation": f"{case}-{driver}-{ordinal}", "census": census})
            for control, reason in controls:
                names.append(f"TestNightlyHistoryRefusalsNoWrites/{driver}/{control}")
                refusals.append(f"{driver}/{control}/{reason}")
        root = ET.Element("testsuites")
        for parent in names[:3]:
            cases_in_suite = [name for name in names if name == parent or name.startswith(parent + "/")]
            suite = ET.SubElement(root, "testsuite", tests=str(len(cases_in_suite)), failures="0", errors="0", skipped="0", name=parent)
            for name in cases_in_suite:
                ET.SubElement(suite, "testcase", classname="nightlyrestore_test", name=name, time="0.01")
        log = "\n".join(f"=== RUN   {name}\n--- PASS: {name} (0.01s)" for name in names) + "\nPASS\n"
        members = {"run.json": encoded(checkout), "exit_code": b"0\n", "test-results/test.xml": ET.tostring(root),
                   "test-results/test.log": log.encode(),
                   "bazel.log": b"//internal/integration/nightlyrestore:nightlyrestore_test PASSED in 50.0s\nExecuted 1 out of 1 test: 1 test passes.\n",
                   "test-results/test.outputs/nightly-restore-diffs.json": encoded({"schema": "plug-nightly-restore-diffs/v1", "status": "bounded_current_state",
                       "source_fixture_sha256": source_hash, "snapshot_fixture_sha256": "a" * 64, "results": snapshots}),
                   "test-results/test.outputs/nightly-history-restore-diffs.json": encoded({"schema": "plug-nightly-history-restore-diffs/v1", "status": "bounded_public_history",
                       "fixtures": {"full": full_hash, "partial": partial_hash}, "results": history}),
                   "test-results/test.outputs/nightly-history-refusals.json": encoded({"schema": "plug-nightly-history-refusals/v1", "status": "pass", "typed_no_write_controls": refusals})}
        (directory / "workflow-run.json").write_bytes(encoded(run))
        artifact = {"id": 80000000001 + index, "name": f"plug-nightly-restore-diffs-{run_id}-1", "expired": False,
                    "workflow_run": {"id": run_id, "repository_id": repo["id"], "head_repository_id": repo["id"], "head_branch": "main", "head_sha": self.head}}
        (directory / "artifact.json").write_bytes(encoded(artifact))
        self.write_zip(directory, members)
        return directory

    def census(self, archive, snapshot=False, members=None):
        ids = ["demo-a", "restore/archive", "instance_config/restore-import/binding", "instance_config/restore-import/project/demo"]
        records = {name: "b" * 64 for name in ids}
        return {"counts": {"bead": 1, "restore-archive": 1, "instance_config": 2}, "ids": ids, "record_sha256": records,
                "version_sha256": records if snapshot else {}, "dependencies": [],
                "source_allocators": [{"prefix": "demo", "id_mode": "hash", "hash_length": 1, "last_id": 8, "child_counters": {}}],
                "target_allocators": [{"prefix": "demo", "id_mode": "random", "random_bytes": 16,
                                       "suffix_encoding": "lower-hex", "top_level_shape": "prefix-random", "child_shape": "parent-random"}],
                "archive_member_sha256": members if members is not None else self.inventories[archive], "archive_sha256": archive}

    def write_zip(self, directory, members):
        with zipfile.ZipFile(directory / "artifact.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in members.items():
                archive.writestr(name, data)
        body = (directory / "artifact.zip").read_bytes()
        artifact = json.loads((directory / "artifact.json").read_bytes())
        artifact.update(digest="sha256:" + digest(body), size_in_bytes=len(body))
        (directory / "artifact.json").write_bytes(encoded(artifact))

    def mutate(self, filename, change, *, member=False, index=2):
        directory = self.receipts[index]
        if member:
            with zipfile.ZipFile(directory / "artifact.zip") as archive:
                members = {name: archive.read(name) for name in archive.namelist()}
            value = json.loads(members[filename])
            change(value)
            members[filename] = encoded(value)
            self.write_zip(directory, members)
        else:
            path = directory / filename
            value = json.loads(path.read_bytes())
            change(value)
            path.write_bytes(encoded(value))

    def result(self, receipts=None):
        return verifier.evaluate(self.receipts if receipts is None else receipts, self.repository, synthetic_tests=True)

    def blocked(self, mutate, reason):
        for path, data in self.originals.items():
            path.write_bytes(data)
        mutate()
        result = self.result()
        self.assertEqual(result["status"], "BLOCK", result)
        self.assertFalse(result["qualified"])
        self.assertIn(reason, result["reason"])

    def edit_members(self, change):
        directory = self.receipts[2]
        with zipfile.ZipFile(directory / "artifact.zip") as archive:
            members = {name: archive.read(name) for name in archive.namelist()}
        change(members)
        self.write_zip(directory, members)

    def test_synthetic_three_consecutive_nights_cross_month_and_order(self):
        result = self.result(list(reversed(self.receipts)))
        self.assertEqual(result["status"], "TEST_PASS", result)
        self.assertFalse(result["qualified"])
        self.assertEqual([night["date"] for night in result["nights"]], ["2026-02-27", "2026-02-28", "2026-03-01"])
        self.assertTrue(all(night["head"] == self.head and night["tree"] == self.tree for night in result["nights"]))

    def test_synthetic_two_nights_wait_and_never_qualify(self):
        for count in (0, 1, 2):
            with self.subTest(count=count):
                result = self.result(self.receipts[:count])
                self.assertEqual(result["status"], "WAIT", result)
                self.assertFalse(result["qualified"])

    def test_duplicate_receipt_is_blocked(self):
        result = self.result([*self.receipts[:2], self.receipts[1]])
        self.assertEqual(result["status"], "BLOCK")
        self.assertIn("duplicate run_id", result["reason"])

    def test_same_night_and_missing_night_are_blocked(self):
        for date, reason in (("2026-02-28", "duplicate date"), ("2026-03-02", "missing consecutive UTC night")):
            with self.subTest(date=date):
                self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run.update(
                    created_at=date + "T04:28:43Z", run_started_at=date + "T04:28:44Z", updated_at=date + "T04:33:27Z")), reason)

    def test_schedule_main_workflow_success_and_attempt_are_required(self):
        for key, value in (("event", "workflow_dispatch"), ("head_branch", "topic"), ("path", ".github/workflows/plug-remote-bazel.yml"),
                           ("status", "in_progress"), ("conclusion", "failure"), ("conclusion", "cancelled"), ("run_attempt", 2), ("run_attempt", True)):
            with self.subTest(key=key, value=value):
                self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run.update({key: value})), "scheduled-main")

    def test_native_artifact_identity_mismatches_are_blocked(self):
        for key, value in (("id", 17), ("head_sha", "e" * 40), ("head_branch", "topic"), ("repository_id", 17), ("head_repository_id", 17)):
            with self.subTest(key=key):
                self.blocked(lambda: self.mutate("artifact.json", lambda artifact: artifact["workflow_run"].update({key: value})), "artifact/run identity")
        for key, value in (("expired", True), ("name", "plug-nightly-restore-diffs-90000000001-2"), ("digest", "sha256:" + "f" * 64), ("size_in_bytes", 1)):
            with self.subTest(key=key):
                self.blocked(lambda: self.mutate("artifact.json", lambda artifact: artifact.update({key: value})), "artifact")

    def test_repository_and_source_object_mismatches_are_blocked(self):
        self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run["repository"].update(full_name="example/other")), "scheduled-main")
        self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run["head_repository"].update(id=17)), "repository identity")
        self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run.pop("created_at")), "timestamp")
        self.blocked(lambda: self.mutate("workflow-run.json", lambda run: run.update(created_at="2026-03-01T23:28:43-05:00")), "UTC")
        # Coherent metadata cannot substitute for an existing immutable source object.
        def missing_source():
            self.mutate("workflow-run.json", lambda run: run.update(head_sha="e" * 40))
            self.mutate("artifact.json", lambda artifact: artifact["workflow_run"].update(head_sha="e" * 40))
            self.mutate("run.json", lambda checkout: checkout.update(head="e" * 40, expected_sha="e" * 40), member=True)
        self.blocked(missing_source, "cat-file")

    def test_distinct_source_heads_resolve_their_own_exact_trees(self):
        path = self.repository / "SYNTHETIC.txt"
        path.write_text("SYNTHETIC second source tree only\n")
        self.git("add", ".")
        self.git("-c", "user.name=Synthetic Evaluator", "-c", "user.email=synthetic@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "SYNTHETIC second source tree only")
        head = self.git("rev-parse", "HEAD").decode().strip()
        tree = self.git("rev-parse", "HEAD^{tree}").decode().strip()
        self.mutate("workflow-run.json", lambda run: run.update(head_sha=head))
        self.mutate("artifact.json", lambda artifact: artifact["workflow_run"].update(head_sha=head))
        self.mutate("run.json", lambda checkout: checkout.update(head=head, expected_sha=head, tree=tree), member=True)
        result = self.result()
        self.assertEqual(result["status"], "TEST_PASS", result)
        self.assertEqual(result["nights"][-1]["tree"], tree)
        self.assertNotEqual(tree, self.tree)

    def test_missing_promisor_object_refuses_without_lazy_fetch(self):
        helper_directory = self.root / "synthetic-remote-helper"
        helper_directory.mkdir()
        marker = self.root / "SYNTHETIC-fetch-attempt"
        helper = helper_directory / "git-remote-n1synthetic"
        helper.write_text("#!/bin/sh\nprintf invoked > " + shlex.quote(str(marker)) + "\nexit 1\n")
        helper.chmod(0o755)
        self.git("config", "core.repositoryformatversion", "1")
        self.git("config", "extensions.partialClone", "origin")
        self.git("config", "remote.origin.promisor", "true")
        self.git("config", "remote.origin.url", "n1synthetic::offline-test")
        self.git("config", "protocol.n1synthetic.allow", "always")
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env["PATH"] = str(helper_directory) + os.pathsep + env["PATH"]
        # Negative control: unprotected Git attempts the local fake transport.
        # The fake helper records an attempt and never accesses a network.
        control = subprocess.run(["git", "-C", str(self.repository), "cat-file", "-t", "e" * 40],
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(control.returncode, 0)
        self.assertTrue(marker.exists(), "negative control did not exercise partial-clone lazy fetch")
        marker.unlink()
        with patch.dict(os.environ, {"PATH": env["PATH"]}):
            with self.assertRaises(subprocess.CalledProcessError):
                verifier.git(self.repository, "cat-file", "-t", "e" * 40)
        self.assertFalse(marker.exists(), "offline object read attempted a fetch")

    def test_ambient_git_object_redirection_cannot_replace_selected_repository(self):
        with patch.dict(os.environ, {"GIT_DIR": str(self.root / "absent.git"), "GIT_OBJECT_DIRECTORY": str(self.root / "absent-objects")}):
            result = self.result()
        self.assertEqual(result["status"], "TEST_PASS", result)

    def test_native_integer_history_revisions_reject_string_and_boolean_substitutes(self):
        path = "test-results/test.outputs/nightly-history-restore-diffs.json"
        for key, value in (("head", "5"), ("floor", "0"), ("changesets_unavailable_through", "0"), ("floor", False)):
            with self.subTest(key=key, value=value):
                self.blocked(lambda: self.mutate(path, lambda report: report["results"][0]["census"].update({key: value}), member=True), "head/floor")

    def test_mixed_immutable_repository_identities_cannot_form_a_streak(self):
        def change():
            self.mutate("workflow-run.json", lambda run: (run["repository"].update(id=17), run["head_repository"].update(id=17)))
            self.mutate("artifact.json", lambda artifact: artifact["workflow_run"].update(repository_id=17, head_repository_id=17))
        self.blocked(change, "mixed immutable repository")

    def test_coherent_group_wide_incomplete_history_is_refused(self):
        path = "test-results/test.outputs/nightly-history-restore-diffs.json"
        cases = [
            (lambda value: value["as_of_record_sha256"].update({"2": {}}), "as-of image"),
            (lambda value: value["versions"]["demo-a"].clear(), "nested version"),
            (lambda value: value["versions"]["demo-a"][1].update(revision=1), "version revision"),
            (lambda value: value["changesets_after_floor"][1].update(revision=1), "duplicate changeset"),
            (lambda value: value["changesets_after_floor"][0]["changes"].clear(), "transition identities"),
            (lambda value: value["current"]["archive_member_sha256"].pop("records/bead.ndjson"), "archive member"),
            (lambda value: value["current"]["source_allocators"].__setitem__(0, {}), "source allocator"),
            (lambda value: value["current"]["target_allocators"].__setitem__(0, {}), "target allocator"),
            (lambda value: value["history_evidence"]["mapping"][0].pop("eventIds"), "replay event"),
        ]
        for edit, reason in cases:
            with self.subTest(reason=reason):
                def change(report):
                    for row in report["results"]:
                        if row["case"] == "strict-default-full":
                            edit(row["census"])
                self.blocked(lambda: self.mutate(path, change, member=True), reason)

    def test_coherent_snapshot_manifest_only_or_empty_allocators_are_refused(self):
        path = "test-results/test.outputs/nightly-restore-diffs.json"
        def manifest_only(report):
            for row in report["results"]:
                members = row["census"]["archive_member_sha256"]
                row["census"]["archive_member_sha256"] = {"manifest.json": members["manifest.json"]}
        self.blocked(lambda: self.mutate(path, manifest_only, member=True), "archive member")
        def empty_allocators(report):
            for row in report["results"]:
                row["census"]["target_allocators"] = [{}]
        self.blocked(lambda: self.mutate(path, empty_allocators, member=True), "target allocator")

    def test_exact_checkout_sha_tree_and_run_bindings_are_required(self):
        for key, value in (("head", "e" * 40), ("expected_sha", "e" * 40), ("tree", "f" * 40), ("checkout_verified", False),
                           ("checkout_verified", 1), ("run_id", "90000000001"), ("run_attempt", "2"), ("event", "workflow_dispatch")):
            with self.subTest(key=key):
                self.blocked(lambda: self.mutate("run.json", lambda run: run.update({key: value}), member=True), "checkout/run binding")

    def test_every_snapshot_driver_and_fresh_restore_is_required(self):
        path = "test-results/test.outputs/nightly-restore-diffs.json"
        for index in range(6):
            with self.subTest(index=index):
                self.blocked(lambda: self.mutate(path, lambda report: report["results"].pop(index), member=True), "driver matrix")
        self.blocked(lambda: self.mutate(path, lambda report: report["results"].append(copy.deepcopy(report["results"][0])), member=True), "driver matrix")
        self.blocked(lambda: self.mutate(path, lambda report: report["results"][0].update(status="failed"), member=True), "did not pass")

    def test_every_history_case_driver_and_fresh_restore_is_required(self):
        path = "test-results/test.outputs/nightly-history-restore-diffs.json"
        for index in range(30):
            with self.subTest(index=index):
                self.blocked(lambda: self.mutate(path, lambda report: report["results"].pop(index), member=True), "driver matrix")
        self.blocked(lambda: self.mutate(path, lambda report: report["results"][0].update(case="invented"), member=True), "unknown history")

    def test_every_typed_no_write_control_and_reason_is_required(self):
        path = "test-results/test.outputs/nightly-history-refusals.json"
        for index in range(15):
            with self.subTest(index=index):
                self.blocked(lambda: self.mutate(path, lambda report: report["typed_no_write_controls"].pop(index), member=True), "typed no-write")
        self.blocked(lambda: self.mutate(path, lambda report: report["typed_no_write_controls"].append(report["typed_no_write_controls"][0]), member=True), "typed no-write")
        self.blocked(lambda: self.mutate(path, lambda report: report["typed_no_write_controls"].__setitem__(0, "memory/default-refuses-partial/unmappable"), member=True), "typed no-write")

    def test_failed_reports_missing_census_and_driver_differences_are_blocked(self):
        prefix = "test-results/test.outputs/"
        for name in ("nightly-restore-diffs.json", "nightly-history-restore-diffs.json", "nightly-history-refusals.json"):
            with self.subTest(name=name):
                self.blocked(lambda: self.mutate(prefix + name, lambda report: report.update(status="failed"), member=True), "failed or unknown report")
        snapshot = prefix + "nightly-restore-diffs.json"
        history = prefix + "nightly-history-restore-diffs.json"
        self.blocked(lambda: self.mutate(snapshot, lambda report: report["results"][0].pop("census"), member=True), "incomplete census")
        self.blocked(lambda: self.mutate(snapshot, lambda report: report["results"][0]["census"]["dependencies"].append("demo-a/blocks/demo-b"), member=True), "census differs")
        self.blocked(lambda: self.mutate(snapshot, lambda report: report.update(source_fixture_sha256="e" * 64), member=True), "fixture/source mismatch")
        self.blocked(lambda: self.mutate(history, lambda report: report["results"][0]["census"].pop("history_evidence"), member=True), "missing replay")
        self.blocked(lambda: self.mutate(history, lambda report: report["results"][0]["census"].update(floor="1"), member=True), "head/floor")

    def test_missing_failed_skipped_duplicate_xml_and_missing_logs_are_blocked(self):
        for filename in ("run.json", "exit_code", "bazel.log", "test-results/test.xml", "test-results/test.log",
                         "test-results/test.outputs/nightly-history-refusals.json"):
            with self.subTest(filename=filename):
                self.blocked(lambda: self.edit_members(lambda members: members.pop(filename)), "missing artifact evidence")
        for tag in ("failure", "error", "skipped"):
            with self.subTest(tag=tag):
                def edit(members):
                    root = ET.fromstring(members["test-results/test.xml"])
                    ET.SubElement(root.find(".//testcase"), tag)
                    members["test-results/test.xml"] = ET.tostring(root)
                self.blocked(lambda: self.edit_members(edit), "test XML contains")
        for duplicate in (False, True):
            with self.subTest(duplicate_xml=duplicate):
                def edit_xml(members):
                    root = ET.fromstring(members["test-results/test.xml"])
                    suite = root.find("testsuite")
                    if duplicate:
                        suite.append(copy.deepcopy(suite.find("testcase")))
                    else:
                        suite.remove(suite.find("testcase"))
                    suite.set("tests", str(len(suite.findall("testcase"))))
                    members["test-results/test.xml"] = ET.tostring(root)
                self.blocked(lambda: self.edit_members(edit_xml), "named XML")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"exit_code": b"3\n"})), "exit is not zero")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"test-results/test.log": b"PASS\n"})), "named RUN")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"test-results/test.log": members["test-results/test.log"].replace(
            b"--- PASS: TestNightlyFreshStoreDiffs (0.01s)\n", b"")})), "named PASS")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"test-results/test.log": members["test-results/test.log"]
            + b"=== RUN   TestNightlyFreshStoreDiffs\n"})), "named RUN")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"bazel.log": b"(cached) PASSED\n"})), "uncached Bazel")

    def test_unsafe_duplicate_json_and_corrupted_artifact_are_blocked(self):
        self.blocked(lambda: self.edit_members(lambda members: members.update({"../run.json": b"{}"})), "unsafe or duplicate")
        self.blocked(lambda: self.edit_members(lambda members: members.update({"run.json": b'{"head":"x","head":"y"}'})), "duplicate JSON key")
        self.blocked(lambda: (self.receipts[2] / "artifact.zip").write_bytes(b"broken zip"), "byte size mismatch")

    def test_normal_cli_refuses_labeled_synthetic_receipts(self):
        result = verifier.evaluate(self.receipts, self.repository)
        self.assertEqual(result["status"], "BLOCK")
        self.assertFalse(result["qualified"])
        output = io.StringIO()
        from contextlib import redirect_stdout
        with redirect_stdout(output):
            code = verifier.main(["--source-repository", str(self.repository), *map(str, self.receipts)])
        self.assertEqual(code, 1)
        self.assertIn("synthetic evaluator records", json.loads(output.getvalue())["reason"])


if __name__ == "__main__":
    unittest.main()
