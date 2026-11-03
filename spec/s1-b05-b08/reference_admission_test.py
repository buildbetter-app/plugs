#!/usr/bin/env python3
"""Disposable admission fixtures, never pinned Beads recordings or clock proof."""

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import warnings
import zipfile

import prepared_reference_test as adapter
import project_reference as independent
import record_reference as recorder
import reference_inputs as inputs


def sha(value):
    return hashlib.sha256(value).hexdigest()


class ReferenceAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="s1-admission-fixture-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def fixture(self):
        # This deliberately tiny source/binary/profile exists only inside a
        # unit test. None is executed or offered as public reference evidence.
        source = self.root / "input-source"
        source.mkdir()
        subprocess.run(["git", "-C", str(source), "init", "--quiet", "--template="],
                       env=inputs.neutral_environment(), check=True)
        (source / "tracked.txt").write_bytes(b"fixture-source\n")
        (source / ".gitignore").write_text("ignored\n")
        inputs.git(source, "add", "tracked.txt", ".gitignore")
        environment = inputs.neutral_environment()
        environment.update({
            "GIT_AUTHOR_NAME": "Fixture", "GIT_COMMITTER_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z",
        })
        subprocess.run(["git", "-c", "core.logAllRefUpdates=false", "-c", "core.hooksPath=/dev/null",
                        "-C", str(source), "commit", "--quiet", "-m", "unit admission fixture"],
                       env=environment, check=True)
        commit = inputs.git(source, "rev-parse", "HEAD").decode().strip()
        tree = inputs.git(source, "rev-parse", "HEAD^{tree}").decode().strip()
        source_manifest = inputs.git(source, "ls-tree", "-r", "--full-tree", "HEAD")
        binary = b"admission fixture; never execute as Beads\n"
        manifest = {
            "source": {"beads": {"commit": commit, "tree": tree}},
            "artifacts": {"bin/bd": {"sha256": sha(binary), "size": len(binary), "mode": "0o755"}},
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
        self.manifest = self.root / "manifest.json"
        self.manifest.write_bytes(manifest_bytes)
        self.archive = self.root / "artifact.zip"
        with zipfile.ZipFile(self.archive, "w") as bundle:
            for name, contents in {
                "bin/bd": binary, "bin/plug": b"unit-fixture", "bundle.tar": b"unit-fixture",
                "manifest.json": manifest_bytes, "receipt.json": manifest_bytes,
            }.items():
                bundle.writestr(name, contents)
        for target, values in (
            (inputs, {"COMMIT": commit, "TREE": tree, "SOURCE_MANIFEST": sha(source_manifest), "SOURCE_ENTRIES": 2}),
            (adapter, {"COMMIT": commit, "TREE": tree, "BINARY_SHA256": sha(binary), "BINARY_SIZE": len(binary),
                       "MANIFEST_SHA256": sha(manifest_bytes), "ZIP_SHA256": adapter.digest_file(self.archive),
                       "ZIP_SIZE": self.archive.stat().st_size}),
        ):
            patch = mock.patch.multiple(target, **values)
            patch.start()
            self.addCleanup(patch.stop)
        self.proof_value = {
            "sourceCommit": commit, "sourceTree": tree, "binarySha256": sha(binary),
            "mode": "go-sql-controlled", "go": {"controlledReads": 1}, "sql": {"controlledReads": 1},
            "scheduleSha256": "a" * 64,
        }
        self.proof = self.root / "fixture-proof.json"
        self.proof.write_text(json.dumps(self.proof_value))
        self.source, self.binary = source, binary

    def consume(self):
        root = self.root / "private"
        root.mkdir()
        return adapter.consume_prepared(root, self.archive, self.source, self.manifest, self.proof)

    def test_missing_clock_proof_refuses_before_output_or_subprocess(self):
        output = self.root / "output"
        with mock.patch.dict(os.environ, {"TEST_UNDECLARED_OUTPUTS_DIR": str(output)}, clear=True), \
                mock.patch.object(adapter.subprocess, "run", side_effect=AssertionError("must not execute")):
            with self.assertRaisesRegex(RuntimeError, "BLOCKED_MISSING_GO_SQL_CLOCK_PROOF"):
                adapter.main()
        self.assertFalse(output.exists())

    def test_clock_admission_rejects_unbound_counts_mode_and_schedule(self):
        self.fixture()
        inputs.verify_clock_proof(self.proof, sha(self.binary))
        variants = []
        for name, value in (("sourceCommit", "0" * 40), ("sourceTree", "0" * 40),
                            ("binarySha256", "0" * 64), ("mode", "go-only"),
                            ("scheduleSha256", ""), ("scheduleSha256", "X" * 64)):
            variant = copy.deepcopy(self.proof_value)
            variant[name] = value
            variants.append(variant)
        for clock in ("go", "sql"):
            for reads in (None, 0, -1, True, "1", 1.0):
                variant = copy.deepcopy(self.proof_value)
                variant[clock]["controlledReads"] = reads
                variants.append(variant)
        for index, variant in enumerate(variants):
            with self.subTest(variant=index):
                self.proof.write_text(json.dumps(variant))
                with self.assertRaises(RuntimeError):
                    inputs.verify_clock_proof(self.proof, sha(self.binary))

    def test_fixed_zip_digest_refuses_before_executable_creation(self):
        self.fixture()
        with self.archive.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(RuntimeError, "ZIP differs"):
            self.consume()
        self.assertFalse((self.root / "private" / "bd").exists())

    def test_manifest_and_duplicate_members_refuse(self):
        self.fixture()
        self.manifest.write_bytes(self.manifest.read_bytes() + b" ")
        with self.assertRaisesRegex(RuntimeError, "manifest differs"):
            adapter.verify_archive(self.archive, self.manifest)
        with zipfile.ZipFile(self.archive) as bundle:
            self.manifest.write_bytes(bundle.read("manifest.json"))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(self.archive, "a") as bundle:
                bundle.writestr("bin/bd", self.binary)
        with mock.patch.multiple(adapter, ZIP_SHA256=adapter.digest_file(self.archive), ZIP_SIZE=self.archive.stat().st_size):
            with self.assertRaisesRegex(RuntimeError, "member set"):
                adapter.verify_archive(self.archive, self.manifest)

    def test_private_source_and_proof_survive_original_mutation(self):
        self.fixture()
        verify = adapter.verify_source

        def mutate_original(private_source):
            (self.source / "tracked.txt").write_bytes(b"original changed\n")
            self.proof.write_text("original changed")
            return verify(private_source)

        with mock.patch.object(adapter, "verify_source", side_effect=mutate_original):
            binary, source, proof, record = self.consume()
        self.assertEqual(binary.read_bytes(), self.binary)
        self.assertEqual((source / "tracked.txt").read_bytes(), b"fixture-source\n")
        self.assertEqual(json.loads(proof.read_bytes()), self.proof_value)
        self.assertEqual(record["clockProofSha256"], sha(proof.read_bytes()))
        self.assertFalse(record["canonicalBazel"])
        self.assertFalse((self.root / "recordings.json").exists())

    def test_ignored_dirt_and_hidden_index_refuse_before_execute(self):
        self.fixture()
        for variant in ("ignored", "hidden", "sparse"):
            with self.subTest(variant=variant):
                if variant == "ignored":
                    (self.source / "ignored").write_text("dirt")
                else:
                    flag = "--assume-unchanged" if variant == "hidden" else "--skip-worktree"
                    inputs.git(self.source, "update-index", flag, "tracked.txt")
                with self.assertRaisesRegex(RuntimeError, "dirty or has hidden"):
                    self.consume()
                self.assertFalse((self.root / "private" / "bd").exists())
                shutil.rmtree(self.root / "private")
                if variant == "ignored":
                    (self.source / "ignored").unlink()
                else:
                    inputs.git(self.source, "update-index", "--no-assume-unchanged", "--no-skip-worktree", "tracked.txt")

    def test_raw_source_bytes_are_checked_even_if_git_status_claims_clean(self):
        self.fixture()
        tracked = self.source / "tracked.txt"
        tracked.write_bytes(b"tampered-bytes\n")
        original_git = inputs.git

        def status_is_clean(source, *arguments):
            return b"" if arguments[0] == "status" else original_git(source, *arguments)

        with mock.patch.object(inputs, "git", side_effect=status_is_clean):
            with self.assertRaisesRegex(RuntimeError, "bytes differ"):
                self.consume()
        self.assertFalse((self.root / "private" / "bd").exists())

    def test_external_git_metadata_and_symlink_inputs_refuse(self):
        self.fixture()
        (self.source / ".git" / "objects" / "info" / "alternates").write_text("/outside\n")
        with self.assertRaisesRegex(RuntimeError, "external or unexpected"):
            self.consume()
        link = self.root / "linked.zip"
        link.symlink_to(self.archive)
        with self.assertRaises(RuntimeError):
            inputs.ordinary_path(str(link))

    def test_ambient_credentials_git_and_python_options_are_not_forwarded(self):
        with mock.patch.dict(os.environ, {
            "GH_TOKEN": "fixture-secret", "GITHUB_TOKEN": "fixture-secret", "GIT_DIR": "/foreign",
            "GIT_CONFIG_COUNT": "1", "PYTHONPATH": "/foreign", "BEADS_DB": "/foreign", "HOME": "/foreign",
        }):
            env = adapter.recorder_environment(Path("/private/source"), Path("/private/proof"))
        for key in ("GH_TOKEN", "GITHUB_TOKEN", "GIT_DIR", "GIT_CONFIG_COUNT", "PYTHONPATH", "BEADS_DB", "HOME"):
            self.assertNotIn(key, env)
        self.assertNotIn("fixture-secret", json.dumps(env))

    def test_json_and_binary_streams_are_lossless(self):
        for stdout, stderr in ((b'{ "ok": true }\r\n', b"warning\xff\n"), (b"\x00\xffraw", b"")):
            with self.subTest(stdout=stdout):
                # A real disposable process exercises the recorder's byte
                # boundary; this process is explicitly not a reference engine.
                program = "import sys;sys.stdout.buffer.write(" + repr(stdout) + ");sys.stderr.buffer.write(" + repr(stderr) + ")"
                commands = []
                recorder.run(Path(sys.executable), self.root, commands, ["-c", program])
                self.assertEqual(base64.b64decode(commands[0]["stdoutBase64"]), stdout)
                self.assertEqual(base64.b64decode(commands[0]["stderrBase64"]), stderr)
                self.assertEqual(commands[0]["stdoutSha256"], sha(stdout))
                self.assertEqual(commands[0]["stderrSha256"], sha(stderr))

    def test_independent_projection_matches_and_retains_prefix(self):
        # Authored projection fixture tests the algorithms only. It is not
        # serialized into reference/provenance.json or offered as a twin.
        vectors = [{"card": "fixture", "ids": ["fixture-a"], "configuration": {"prefix": "fixture"},
                    "commands": [{"argv": ["probe"], "exitCode": 0}],
                    "observations": {"probeResults": [{"stdoutBase64": ""}]}}]
        projected = independent.project(vectors)
        self.assertEqual(projected, recorder.project(vectors))
        changed = copy.deepcopy(vectors)
        changed[0]["configuration"]["prefix"] = "different"
        self.assertNotEqual(projected, independent.project(changed))

    def test_independent_projection_rejects_corrupt_streams_digests_and_twins(self):
        command = {"argv": ["unit-probe"], "exitCode": 0, "stdout": {"fixture": True},
                   "stdoutBase64": base64.b64encode(b'{ "fixture": true }\n').decode(),
                   "stdoutSha256": sha(b'{ "fixture": true }\n'),
                   "stderrBase64": "", "stderrSha256": sha(b"")}
        vectors = [{"card": card, "ids": ["unit-a"], "configuration": {"prefix": "unit"},
                    "commands": [command], "observations": {"probeResults": [command]}}
                   for card in ("B05", "B06", "B07", "B08")]
        value = {"status": "recorded", "recordings": [
            {"run": run, "raw": copy.deepcopy(vectors),
             "rawSha256": sha(recorder.canonical(vectors).encode()),
             "projectionSha256": sha(recorder.canonical(recorder.project(vectors)).encode())}
            for run in ("a", "b")
        ]}
        independent.verify_recordings(value)
        for variant in ("stream", "parsed", "raw", "projection", "missing-twin", "different-twin"):
            with self.subTest(variant=variant):
                changed = copy.deepcopy(value)
                first = changed["recordings"][0]
                if variant == "stream":
                    first["raw"][0]["commands"][0]["stdoutBase64"] = "Y2hhbmdlZA=="
                elif variant == "parsed":
                    first["raw"][0]["commands"][0]["stdout"] = {"fixture": False}
                elif variant == "raw":
                    first["rawSha256"] = "0" * 64
                elif variant == "projection":
                    first["projectionSha256"] = "0" * 64
                elif variant == "missing-twin":
                    changed["recordings"].pop()
                else:
                    second = changed["recordings"][1]
                    second["raw"][0]["ids"] = ["different-unit-a"]
                    second["rawSha256"] = sha(recorder.canonical(second["raw"]).encode())
                    second["projectionSha256"] = sha(recorder.canonical(recorder.project(second["raw"])).encode())
                with self.assertRaises((RuntimeError, json.JSONDecodeError)):
                    independent.verify_recordings(changed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
