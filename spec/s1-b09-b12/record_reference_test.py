#!/usr/bin/env python3
"""Recorder policy regressions; these fixtures are never Beads evidence."""

import base64
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import record_reference as reference


class RecorderPolicyTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.work_patch = patch.object(reference, "WORK_ROOT", self.root / "work")
        self.work_patch.start()
        self.addCleanup(self.work_patch.stop)
        self.addCleanup(self.directory.cleanup)

    def make_source(self):
        source = self.root / "source"
        source.mkdir()
        env = reference.isolated_environment(self.root / "git-environment", "test-source")

        def git(*args):
            return subprocess.check_output([reference.GIT, "-C", str(source), *args], env=env)

        git("init", "-q")
        (source / "tracked.go").write_text("package example\n", encoding="utf-8")
        git("add", "tracked.go")
        git("commit", "-qm", "Recorder policy test source")
        commit = git("rev-parse", "HEAD").decode().strip()
        tree = git("rev-parse", "HEAD^{tree}").decode().strip()
        self.pin_patch = patch.object(reference, "PIN", commit)
        self.tree_patch = patch.object(reference, "TREE", tree)
        self.pin_patch.start()
        self.tree_patch.start()
        self.addCleanup(self.pin_patch.stop)
        self.addCleanup(self.tree_patch.stop)
        return source, commit, tree

    def test_clean_source_is_bound_to_commit_and_tree(self):
        source, commit, tree = self.make_source()
        self.assertEqual(reference.verify_source(source), {
            "commit": commit, "tree": tree, "cleanWorkingTree": True,
        })

    def test_modified_tracked_source_is_rejected_before_evidence_hashing(self):
        source, _, _ = self.make_source()
        (source / "tracked.go").write_text("package tampered\n", encoding="utf-8")
        with patch.object(reference, "digest", side_effect=AssertionError("hashed dirty source")):
            with self.assertRaisesRegex(RuntimeError, "source checkout is dirty"):
                reference.source_evidence(source, "B09")

    def test_untracked_source_is_rejected_without_leaking_filename(self):
        source, _, _ = self.make_source()
        (source / "unrelated-private-name").write_text("test fixture only\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "source checkout is dirty") as error:
            reference.verify_source(source)
        self.assertNotIn("unrelated-private-name", str(error.exception))

    def test_ambient_credentials_and_git_configuration_are_not_inherited(self):
        ambient_home = self.root / "ambient-home"
        ambient_home.mkdir()
        (ambient_home / ".gitconfig").write_text(
            "[user]\n name = ambient-user\n[credential]\n helper = ambient-helper\n",
            encoding="utf-8",
        )
        ambient = {
            "HOME": str(ambient_home), "XDG_CONFIG_HOME": str(ambient_home),
            "PATH": "/nonexistent", "GIT_DIR": "/ambient/repository",
            "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "user.name",
            "GIT_CONFIG_VALUE_0": "ambient-user", "GIT_ASKPASS": "/ambient/askpass",
            "GH_TOKEN": "test-ambient-value", "GITHUB_TOKEN": "test-ambient-value",
            "AWS_ACCESS_KEY_ID": "test-ambient-value", "AWS_SESSION_TOKEN": "test-ambient-value",
            "GOOGLE_APPLICATION_CREDENTIALS": "/ambient/credentials.json",
            "SSH_AUTH_SOCK": "/ambient/ssh-agent", "DOLT_ROOT_PATH": "/ambient/dolt",
            "BEADS_DIR": "/ambient/beads", "ANTHROPIC_API_KEY": "test-ambient-value",
            "HTTP_PROXY": "http://ambient.invalid", "BD_CONFIG": "/ambient/config",
        }
        with patch.dict(os.environ, ambient, clear=True):
            recorder = reference.Recorder(self.root / "unused-binary", self.root, "B09")
            self.addCleanup(shutil.rmtree, recorder.workspace, True)
            for key in ambient.keys() - {"HOME", "XDG_CONFIG_HOME", "PATH", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0", "GIT_ASKPASS"}:
                self.assertNotIn(key, recorder.env)
            self.assertNotEqual(recorder.env["HOME"], str(ambient_home))
            for key in ("HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "TMPDIR"):
                self.assertTrue(Path(recorder.env[key]).is_relative_to(recorder.workspace))
                self.assertEqual(Path(recorder.env[key]).stat().st_mode & 0o777, 0o700)
            self.assertEqual(recorder.env["GIT_TERMINAL_PROMPT"], "0")
            self.assertEqual(recorder.env["GIT_ASKPASS"], "/bin/false")
            self.assertEqual(recorder.env["GIT_SSH_COMMAND"], "/bin/false")
            for key, expected in (("user.name", "s1-b09-b12-reference"), ("credential.helper", "")):
                actual = subprocess.check_output([reference.GIT, "config", "--get", key], cwd=recorder.workspace, env=recorder.env).decode().strip()
                self.assertEqual(actual, expected)

    def recorded_fixture(self, stdout, stderr=b"", stdin=None):
        recorder = reference.Recorder(self.root / "unused-binary", self.root, "B09")
        self.addCleanup(shutil.rmtree, recorder.workspace, True)
        result = subprocess.CompletedProcess([], 0, stdout, stderr)
        with patch.object(reference.subprocess, "run", return_value=result) as run:
            returned = recorder.run("fixture", ["fixture"], stdin=stdin)
        self.assertEqual(returned, (0, stdout, stderr))
        self.assertEqual(run.call_args.kwargs["env"], recorder.env)
        return recorder, run.call_args.kwargs

    def test_exact_stream_bytes_survive_decoding_and_normalization(self):
        stdout, stderr = b"\xff\x00\r\n", b"error\xfe\r\n"
        recorder, invocation = self.recorded_fixture(stdout, stderr, stdin="input")
        stream = recorder.streams[0]
        self.assertEqual(base64.b64decode(stream["stdoutBase64"], validate=True), stdout)
        self.assertEqual(base64.b64decode(stream["stderrBase64"], validate=True), stderr)
        self.assertEqual(stream["stdoutSha256"], reference.digest(stdout))
        self.assertEqual(stream["stderrSha256"], reference.digest(stderr))
        self.assertEqual(invocation["input"], b"input")
        self.assertNotEqual(recorder.commands[0]["stdout"].encode(), stdout)

    def test_raw_inequality_is_not_promoted_to_normalized_equality(self):
        first, _ = self.recorded_fixture(b'{"id":"b09-abc","created_at":"2026-11-02T12:00:00Z"}\n')
        repeat, _ = self.recorded_fixture(b'{"id":"b09-def","created_at":"2026-11-02T12:00:01Z"}\r\n')
        def payload(recorder):
            return {"commands": recorder.commands, "streams": recorder.streams, "observations": {}, "projection": {"fixture": True}}
        equality = reference.compare_runs(payload(first), payload(repeat))
        self.assertTrue(equality["normalizedTranscriptsEqual"])
        self.assertTrue(equality["projectionsEqual"])
        self.assertFalse(equality["rawStreamsEqual"])
        self.assertEqual(equality["perCommand"], [{"label": "fixture", "stdoutEqual": False, "stderrEqual": True}])
        self.assertNotEqual(reference.digest(first.streams), reference.digest(repeat.streams))

    def test_normalized_semantic_difference_is_rejected(self):
        first, _ = self.recorded_fixture(b'{"title":"first"}\n')
        repeat, _ = self.recorded_fixture(b'{"title":"second"}\n')
        def payload(recorder):
            return {"commands": recorder.commands, "streams": recorder.streams, "observations": {}, "projection": {}}
        with self.assertRaisesRegex(RuntimeError, "normalized independent source transcripts differ"):
            reference.compare_runs(payload(first), payload(repeat))


if __name__ == "__main__":
    unittest.main(verbosity=2)
