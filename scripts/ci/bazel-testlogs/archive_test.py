#!/usr/bin/env python3
"""Exercise the workflow's real archive block with temporary output bytes.

These fixtures test artifact transport only. They are not public RPC captures
or expected B13 readback, and cannot qualify the B13 product behavior.
"""

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import warnings
import zipfile


HERE = Path(__file__).absolute().parent
if "TEST_SRCDIR" in os.environ:
    REPOSITORY = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
else:
    REPOSITORY = HERE.parents[2]
WORKFLOW = REPOSITORY / ".github/workflows/plug-remote-bazel.yml"
INVENTORY = "undeclared-outputs.json"
B13_TEST = "internal/integration/b13memory/b13memory_test"
SINKS = tuple(
    "b13-readback-" + engine + "-" + side + "-public.jsonl"
    for engine in ("memory", "sqlite", "postgres")
    for side in ("source", "target")
)


def workflow_archive_block():
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("      - name: Archive Bazel test logs")
    end = next(index for index in range(start + 1, len(lines)) if lines[index].startswith("      - name:"))
    step = lines[start:end]
    if "        if: always()" not in step:
        raise AssertionError("test output transport must run after a failed test")
    run = step.index("        run: |") + 1
    return "\n".join(line[10:] for line in step[run:] if line) + "\n"


class ArchiveTransportTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="plug-bazel-outputs-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.logs = self.root / "bazel-testlogs"
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()
        self.candidate_marker = self.root / "candidate-archive-helper-used"
        candidate = self.root / "scripts/ci/bazel-testlogs/archive.py"
        candidate.parent.mkdir(parents=True)
        candidate.write_text(
            "import os\n"
            "from pathlib import Path\n"
            "Path(os.environ['CANDIDATE_ARCHIVE_MARKER']).write_text('candidate\\n')\n"
            "raise SystemExit('candidate archive helper must not run')\n"
        )
        trusted = self.root / "trusted-ci/scripts/ci/bazel-testlogs/archive.py"
        trusted.parent.mkdir(parents=True)
        shutil.copyfile(HERE / "archive.py", trusted)

    def write(self, path, data):
        destination = self.logs / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return destination

    def archive(self, expected_exit=0, resolution_error=None):
        environment = dict(
            os.environ,
            GITHUB_WORKSPACE=str(self.root),
            REMOTE_BAZEL_EVIDENCE_DIR=str(self.evidence),
            CANDIDATE_ARCHIVE_MARKER=str(self.candidate_marker),
        )
        if resolution_error is not None:
            # Exercise Python 3.12's RuntimeError path on every test interpreter,
            # while running the unchanged helper via the actual workflow block.
            mocks = self.root / "resolution-error"
            mocks.mkdir(exist_ok=True)
            (mocks / "sitecustomize.py").write_text(
                "from pathlib import Path\n"
                "original_resolve = Path.resolve\n"
                "def fixture_resolve(self, *args, **kwargs):\n"
                f"    if self.absolute() == Path({str(resolution_error)!r}):\n"
                "        raise RuntimeError('fixture Python 3.12 symlink resolution loop')\n"
                "    return original_resolve(self, *args, **kwargs)\n"
                "Path.resolve = fixture_resolve\n"
            )
            environment["PYTHONPATH"] = str(mocks)
        result = subprocess.run(
            ["bash", "-euo", "pipefail", "-c", workflow_archive_block()],
            cwd=self.root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.assertEqual(result.returncode, expected_exit, result.stderr.decode(errors="replace"))
        with tarfile.open(self.evidence / "bazel-testlogs.tgz", "r:gz") as archive:
            names = [member.name for member in archive.getmembers()]
            self.assertEqual(len(names), len(set(names)), "archive paths must not overwrite another configuration")
            files = {member.name.removeprefix("./"): archive.extractfile(member).read()
                     for member in archive.getmembers() if member.isfile()}
        self.assertIn(INVENTORY, files, "standard artifact must retain the actual output inventory")
        inventory = json.loads(files[INVENTORY])
        for record in inventory["files"]:
            self.assertEqual(record["bytes"], len(files[record["path"]]))
            self.assertEqual(record["sha256"], hashlib.sha256(files[record["path"]]).hexdigest())
        return files, inventory

    def test_archive_step_uses_trusted_checkout_helper(self):
        block = workflow_archive_block()
        self.assertIn(
            'python3 "$GITHUB_WORKSPACE/trusted-ci/scripts/ci/bazel-testlogs/archive.py"',
            block,
        )
        self.assertNotIn("python3 scripts/ci/bazel-testlogs/archive.py", block)
        self.archive()
        self.assertFalse(self.candidate_marker.exists())

    def zipped(self, name, contents):
        raw = io.BytesIO()
        with zipfile.ZipFile(raw, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path, data in contents:
                archive.writestr(path, data)
        path = name + "/test.outputs/outputs.zip"
        self.write(path, raw.getvalue())
        return path, raw.getvalue()

    def assert_original_outputs(self, files, inventory, expected):
        observed = {}
        for record in inventory["outputs"]:
            if "container" in record:
                with zipfile.ZipFile(io.BytesIO(files[record["container"]])) as archive:
                    info = archive.infolist()[record["member_index"]]
                    data = archive.read(info)
                    self.assertEqual(info.filename, record["name"])
            else:
                data = files[record["path"]]
            identity = (record["test"], record["name"])
            self.assertNotIn(identity, observed)
            observed[identity] = data
            self.assertEqual(record["bytes"], len(data))
            self.assertEqual(record["sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(observed, expected)

    def test_six_zipped_sinks_keep_exact_bytes_after_success_and_failure(self):
        # Deliberately opaque transport bytes, never a claimed public capture.
        sinks = {name: (name + " Ω\n").encode() + b"\x00\xff\r\n\t" for name in SINKS}
        for failed in (False, True):
            with self.subTest(test_failed=failed):
                log = b"--- FAIL: transport-fixture\n" if failed else b"--- PASS: transport-fixture\n"
                xml = b'<testsuite failures="1"/>\n' if failed else b'<testsuite failures="0"/>\n'
                self.write(B13_TEST + "/test.log", log)
                self.write(B13_TEST + "/test.xml", xml)
                (self.evidence / "exit_code").write_bytes(b"3\n" if failed else b"0\n")
                path, original_zip = self.zipped(B13_TEST, sinks.items())
                manifest = b"opaque Bazel undeclared output manifest\n"
                self.write(B13_TEST + "/test.outputs_manifest/MANIFEST", manifest)
                files, inventory = self.archive()
                self.assertTrue(inventory["complete"])
                self.assertEqual(inventory["errors"], [])
                self.assertEqual(files[path], original_zip)
                self.assertEqual(files[B13_TEST + "/test.log"], log)
                self.assertEqual(files[B13_TEST + "/test.xml"], xml)
                self.assertEqual(files[B13_TEST + "/test.outputs_manifest/MANIFEST"], manifest)
                self.assertEqual((self.evidence / "exit_code").read_bytes(), b"3\n" if failed else b"0\n")
                self.assert_original_outputs(files, inventory, {(B13_TEST, name): data for name, data in sinks.items()})

    def test_unzipped_nested_and_empty_outputs_keep_exact_names_and_bytes(self):
        contents = {"raw Ω/source public.jsonl": b"one\r\n\x00two\n", "empty.jsonl": b""}
        for name, data in contents.items():
            self.write(B13_TEST + "/test.outputs/" + name, data)
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assert_original_outputs(files, inventory, {(B13_TEST, name): data for name, data in contents.items()})

    def test_shards_and_attempts_keep_same_named_outputs_distinct(self):
        expected = {}
        for attempt in ("shard_1_of_2", "shard_2_of_2", "test_attempts/attempt_1"):
            test = B13_TEST + "/" + attempt
            data = attempt.encode() + b"\n"
            self.zipped(test, [(SINKS[0], data)])
            expected[(test, SINKS[0])] = data
        # Bazel can also name a retried action's outputs beside attempt_1.log.
        raw = io.BytesIO()
        with zipfile.ZipFile(raw, "w") as archive:
            archive.writestr(SINKS[0], b"attempt_2 original output\n")
        self.write(B13_TEST + "/test_attempts/attempt_2.outputs/outputs.zip", raw.getvalue())
        self.write(B13_TEST + "/test_attempts/attempt_2.log", b"actual failed attempt log\n")
        expected[(B13_TEST + "/test_attempts/attempt_2", SINKS[0])] = b"attempt_2 original output\n"
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual(files[B13_TEST + "/test_attempts/attempt_2.log"], b"actual failed attempt log\n")
        self.assert_original_outputs(files, inventory, expected)

    def test_bazel_workspace_convenience_symlink_keeps_original_outputs(self):
        self.write(B13_TEST + "/test.outputs/" + SINKS[0], b"original sink bytes\n")
        real_logs = self.root / "output-base/testlogs"
        real_logs.parent.mkdir()
        self.logs.rename(real_logs)
        self.logs.symlink_to(real_logs, target_is_directory=True)
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assert_original_outputs(files, inventory, {(B13_TEST, SINKS[0]): b"original sink bytes\n"})

    def configured_logs(self):
        output_tree = self.root / "output-base/execroot/_main/bazel-out"
        default_logs = output_tree / "k8-fastbuild/testlogs"
        default_logs.parent.mkdir(parents=True)
        if self.logs.exists():
            self.logs.rename(default_logs)
        else:
            default_logs.mkdir()
        self.logs.symlink_to(default_logs, target_is_directory=True)
        return output_tree

    def write_configuration(self, output_tree, configuration, path, data):
        destination = output_tree / configuration / "testlogs" / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return destination

    def test_configured_doc_and_companions_keep_own_raw_logs_and_xml(self):
        # Transport fixtures model the actual split that omitted the doc target;
        # these bytes are not claimed as execution of the Beads doc suite.
        expected = {}
        for target in ("scripts/scripts_test", "scripts/go_test_sources_test"):
            for suffix, data in (("test.log", (target + " original log\n").encode()),
                                 ("test.xml", ("<testsuite name='" + target + "'/>\n").encode())):
                path = target + "/" + suffix
                self.write(path, data)
                expected[path] = data
        output_tree = self.configured_logs()
        configuration = "k8-fastbuild-ST-doc-integration"
        prefix = "_configurations/" + configuration
        for suffix, data in (("test.log", b"original configured doc log\n"),
                             ("test.xml", b"<testsuite name='configured-doc'/>\n")):
            path = "scripts/doc_freshness_required_test/" + suffix
            self.write_configuration(output_tree, configuration, path, data)
            expected[prefix + "/" + path] = data
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual({name: data for name, data in files.items() if name != INVENTORY}, expected)
        self.assertEqual(inventory["testlog_roots"], [
            {"configuration": "k8-fastbuild", "archive_prefix": ""},
            {"configuration": configuration, "archive_prefix": prefix},
        ])

    def test_same_label_in_two_configurations_keeps_distinct_results_and_outputs(self):
        target = "scripts/same_test"
        self.write(target + "/test.log", b"default original log\n")
        self.write(target + "/test.xml", b"<testsuite name='default'/>\n")
        self.write(target + "/test.outputs/raw.bin", b"default original output\n")
        output_tree = self.configured_logs()
        configuration = "k8-fastbuild-ST-integration"
        configured = "_configurations/" + configuration + "/" + target
        self.write_configuration(output_tree, configuration, target + "/test.xml", b"<testsuite name='configured'/>\n")
        self.write_configuration(output_tree, configuration, target + "/test.outputs/raw.bin", b"configured original output\n")
        attempt = self.write_configuration(output_tree, configuration, target + "/test_attempts/attempt_1.log", b"configured original log\n")
        (attempt.parent.parent / "test.log").symlink_to("test_attempts/attempt_1.log")
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual(files[target + "/test.log"], b"default original log\n")
        self.assertEqual(files[configured + "/test.log"], b"configured original log\n")
        self.assertEqual(files[target + "/test.xml"], b"<testsuite name='default'/>\n")
        self.assertEqual(files[configured + "/test.xml"], b"<testsuite name='configured'/>\n")
        record = next(row for row in inventory["files"] if row["path"] == configured + "/test.log")
        self.assertEqual(record["snapshot_source"], configured + "/test_attempts/attempt_1.log")
        self.assert_original_outputs(files, inventory, {
            (target, "raw.bin"): b"default original output\n",
            (configured, "raw.bin"): b"configured original output\n",
        })

    def test_configuration_alias_does_not_duplicate_the_convenience_tree(self):
        path = "scripts/original_test/test.log"
        self.write(path, b"one original log\n")
        output_tree = self.configured_logs()
        (output_tree / "alias").symlink_to("k8-fastbuild", target_is_directory=True)
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual(set(files), {path, INVENTORY})
        self.assertEqual(inventory["testlog_roots"], [{"configuration": "k8-fastbuild", "archive_prefix": ""}])

    def test_configured_testlog_escape_keeps_default_logs_and_fails_collection(self):
        path = "scripts/original_test/test.log"
        self.write(path, b"original default log\n")
        output_tree = self.configured_logs()
        unrelated = self.root / "unrelated/testlogs"
        unrelated.mkdir(parents=True)
        (unrelated / "secret").write_bytes(b"must not be archived\n")
        configuration = output_tree / "k8-fastbuild-ST-escape"
        configuration.mkdir()
        (configuration / "testlogs").symlink_to(unrelated, target_is_directory=True)
        files, inventory = self.archive(expected_exit=86)
        self.assertFalse(inventory["complete"])
        self.assertEqual(set(files), {path, INVENTORY})
        self.assertEqual(files[path], b"original default log\n")
        self.assertIn("inside the Bazel output tree", inventory["errors"][0]["error"])

    def test_configured_archive_collision_is_explicit_and_never_overwrites(self):
        configuration = "k8-fastbuild-ST-collision"
        relative = "scripts/same_test/test.log"
        path = "_configurations/" + configuration + "/" + relative
        self.write(path, b"default original bytes\n")
        output_tree = self.configured_logs()
        self.write_configuration(output_tree, configuration, relative, b"configured conflicting bytes\n")
        files, inventory = self.archive(expected_exit=86)
        self.assertFalse(inventory["complete"])
        self.assertEqual(files[path], b"default original bytes\n")
        self.assertEqual(inventory["errors"][0]["path"], path)
        self.assertIn("duplicate configured archive path", inventory["errors"][0]["error"])

    def test_missing_testlogs_keeps_empty_inventory_without_invented_sinks(self):
        files, inventory = self.archive()
        self.assertEqual(set(files), {INVENTORY})
        self.assertEqual(inventory["outputs"], [])
        self.assertEqual(inventory["files"], [])

    def test_missing_sinks_are_not_filled_from_expected_names(self):
        self.zipped(B13_TEST, [(SINKS[0], b"actual partial output\n")])
        files, inventory = self.archive()
        self.assertEqual(len(inventory["outputs"]), 1)
        self.assert_original_outputs(files, inventory, {(B13_TEST, SINKS[0]): b"actual partial output\n"})

    def test_corrupt_zip_is_retained_and_archive_step_fails(self):
        path = B13_TEST + "/test.outputs/outputs.zip"
        original = b"not a ZIP: retain these original bytes\n"
        self.write(path, original)
        self.write(B13_TEST + "/test.log", b"--- FAIL: original failure\n")
        for original_exit in (b"0\n", b"3\n"):
            with self.subTest(original_bazel_exit=original_exit):
                (self.evidence / "exit_code").write_bytes(original_exit)
                files, inventory = self.archive(expected_exit=86)
                self.assertFalse(inventory["complete"])
                self.assertEqual(files[path], original)
                self.assertEqual(inventory["errors"][0]["path"], path)
                self.assertIn(B13_TEST + "/test.log", files)
                self.assertEqual((self.evidence / "exit_code").read_bytes(), original_exit)

    def test_unsafe_and_symlink_zip_members_are_refused_without_extraction(self):
        for name in ("../escape.jsonl", "/absolute.jsonl", "bad\\name.jsonl", "symbolic"):
            with self.subTest(name=name):
                raw = io.BytesIO()
                with zipfile.ZipFile(raw, "w") as archive:
                    info = zipfile.ZipInfo(name)
                    if name == "symbolic":
                        info.create_system, info.external_attr = 3, 0o120777 << 16
                    archive.writestr(info, b"original output bytes\n")
                path = B13_TEST + "/test.outputs/outputs.zip"
                self.write(path, raw.getvalue())
                files, inventory = self.archive(expected_exit=86)
                self.assertFalse(inventory["complete"])
                self.assertEqual(files[path], raw.getvalue())
                self.assertEqual(inventory["outputs"], [])
        self.assertFalse((self.root / "escape.jsonl").exists())

    def test_safe_zip_directory_metadata_is_checked_without_being_indexed(self):
        path, original = self.zipped(B13_TEST, [("nested/", b""), ("nested/raw.bin", b"opaque\x00\xff\n")])
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual(files[path], original)
        self.assert_original_outputs(files, inventory, {(B13_TEST, "nested/raw.bin"): b"opaque\x00\xff\n"})
        self.assertEqual(inventory["outputs"][0]["member_index"], 1)

    def test_unsafe_zip_directories_retain_bytes_and_fail_before_metadata_skip(self):
        log = b"--- FAIL: original test result\n"
        self.write(B13_TEST + "/test.log", log)
        self.write(B13_TEST + "/test.xml", b'<testsuite failures="1"/>\n')
        survivor = B13_TEST + "/test.outputs/survivor.bin"
        self.write(survivor, b"surviving opaque output\x00\xff\n")
        cases = (("../outside/",), ("/absolute/",), ("bad\\directory/",), ("symbolic/",), ("duplicate/", "duplicate/"))
        for names in cases:
            with self.subTest(directory_entries=names):
                raw = io.BytesIO()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    with zipfile.ZipFile(raw, "w") as archive:
                        for name in names:
                            info = zipfile.ZipInfo(name)
                            info.create_system = 3
                            info.external_attr = (0o120777 if name == "symbolic/" else 0o40755) << 16
                            archive.writestr(info, b"")
                path = B13_TEST + "/test.outputs/outputs.zip"
                self.write(path, raw.getvalue())
                for original_exit in (b"0\n", b"3\n"):
                    with self.subTest(original_bazel_exit=original_exit):
                        (self.evidence / "exit_code").write_bytes(original_exit)
                        files, inventory = self.archive(expected_exit=86)
                        self.assertFalse(inventory["complete"])
                        self.assertEqual(files[path], raw.getvalue())
                        self.assertEqual(files[B13_TEST + "/test.log"], log)
                        self.assertTrue(any(row["path"] == path and "ambiguous" in row["error"] for row in inventory["errors"]))
                        self.assert_original_outputs(files, inventory, {(B13_TEST, "survivor.bin"): b"surviving opaque output\x00\xff\n"})
                        self.assertEqual((self.evidence / "exit_code").read_bytes(), original_exit)
        self.assertFalse((self.root / "outside").exists())

    def test_runtime_resolution_error_keeps_surviving_originals_and_incomplete_archive(self):
        path, original = self.zipped(B13_TEST, [("raw.bin", b"original container member\x00\xff\n")])
        log = b"--- FAIL: original test result\n"
        xml = b'<testsuite failures="1"/>\n'
        self.write(B13_TEST + "/test.log", log)
        self.write(B13_TEST + "/test.xml", xml)
        loop = self.logs / B13_TEST / "test.outputs/resolution-loop"
        loop.symlink_to(loop.name)
        for original_exit in (b"0\n", b"3\n"):
            with self.subTest(original_bazel_exit=original_exit):
                (self.evidence / "exit_code").write_bytes(original_exit)
                files, inventory = self.archive(expected_exit=86, resolution_error=loop)
                self.assertFalse(inventory["complete"])
                self.assertEqual(files[path], original)
                self.assertEqual(files[B13_TEST + "/test.log"], log)
                self.assertEqual(files[B13_TEST + "/test.xml"], xml)
                self.assertNotIn(loop.relative_to(self.logs).as_posix(), files)
                self.assertTrue(any(row["path"] == loop.relative_to(self.logs).as_posix()
                                    and "fixture Python 3.12" in row["error"] for row in inventory["errors"]))
                self.assert_original_outputs(files, inventory, {(B13_TEST, "raw.bin"): b"original container member\x00\xff\n"})
                self.assertEqual((self.evidence / "exit_code").read_bytes(), original_exit)

    def test_root_resolution_error_publishes_an_explicit_incomplete_archive(self):
        self.logs.symlink_to(self.logs.name, target_is_directory=True)
        for original_exit in (b"0\n", b"3\n"):
            with self.subTest(original_bazel_exit=original_exit):
                (self.evidence / "exit_code").write_bytes(original_exit)
                files, inventory = self.archive(expected_exit=86, resolution_error=self.logs)
                self.assertEqual(set(files), {INVENTORY})
                self.assertFalse(inventory["complete"])
                self.assertEqual(inventory["files"], [])
                self.assertEqual(inventory["outputs"], [])
                self.assertEqual(inventory["errors"][0]["path"], ".")
                self.assertIn("fixture Python 3.12", inventory["errors"][0]["error"])
                self.assertEqual((self.evidence / "exit_code").read_bytes(), original_exit)

    def test_duplicate_zip_member_retains_container_and_reports_incomplete(self):
        raw = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(raw, "w") as archive:
                archive.writestr(SINKS[0], b"first actual member\n")
                archive.writestr(SINKS[0], b"second actual member\n")
        path = B13_TEST + "/test.outputs/outputs.zip"
        self.write(path, raw.getvalue())
        files, inventory = self.archive(expected_exit=86)
        self.assertFalse(inventory["complete"])
        self.assertEqual(files[path], raw.getvalue())
        self.assertIn("ambiguous", inventory["errors"][0]["error"])

    def test_nested_output_symlinks_do_not_read_unrelated_files(self):
        unrelated = self.root / "unrelated.jsonl"
        unrelated.write_bytes(b"outside the Bazel output tree\n")
        output = self.logs / B13_TEST / "test.outputs" / SINKS[0]
        output.parent.mkdir(parents=True)
        output.symlink_to(unrelated)
        files, inventory = self.archive(expected_exit=86)
        self.assertFalse(inventory["complete"])
        self.assertNotIn(str(output.relative_to(self.logs)), files)
        self.assertEqual(inventory["outputs"], [])

    def test_bazel_internal_log_alias_keeps_bytes_and_records_original_link(self):
        path = B13_TEST + "/test_attempts/attempt_1.log"
        self.write(path, b"original failing attempt bytes\n")
        alias = self.logs / B13_TEST / "test.log"
        alias.symlink_to("test_attempts/attempt_1.log")
        files, inventory = self.archive()
        self.assertTrue(inventory["complete"])
        self.assertEqual(files[B13_TEST + "/test.log"], files[path])
        record = next(row for row in inventory["files"] if row["path"] == B13_TEST + "/test.log")
        self.assertEqual(record["original_symlink"], "test_attempts/attempt_1.log")
        self.assertEqual(record["snapshot_source"], path)


if __name__ == "__main__":
    unittest.main()
