#!/usr/bin/env python3
"""Offline policy checks for the B23 recorder/extractor boundary."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import extract_ordinary_count_packet as extract
import ordinary_count_profile as profile
import record_ordinary_count as recorder


class PolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        recorder.WORK_ROOT.mkdir(parents=True, exist_ok=True)

    def directory(self, prefix="s1-b23-policy-"):
        root = Path(tempfile.mkdtemp(prefix=prefix, dir=recorder.WORK_ROOT))
        self.addCleanup(shutil.rmtree, root, True)
        return root

    def frame(self, packet):
        payload = recorder.canonical(packet)
        identity = f"{len(payload)} {recorder.digest(payload)}".encode()
        return profile.PROFILE["capture"]["frameBegin"].encode() + b" " + identity + b"\n" + payload + b"\n" + profile.PROFILE["capture"]["frameEnd"].encode() + b" " + identity + b"\n"

    def test_frame_rejects_duplicate_missing_or_changed_bytes(self):
        packet = {"status": "BLOCK", "bytes": "fixture"}
        good = self.frame(packet)
        extract.unframe(good)
        # Real Go test logs have both a prefix and a suffix outside the frame.
        extract.unframe(b"=== RUN fixture\n" + good + b"--- PASS fixture\n")
        for bad in (good + good, good.replace(b"fixture", b"changed"), good.replace(profile.PROFILE["capture"]["frameEnd"].encode(), b"wrong-end"), good.replace(b"\n", b"\r\n"), good[:-1]):
            with self.subTest(bad=bad[:40]), self.assertRaises(ValueError):
                extract.unframe(bad)

    def test_private_config_is_exact_and_existing_file_is_not_reused(self):
        root = self.directory()
        runtime = root / "runtime-bin" / "dolt"
        runtime.parent.mkdir()
        runtime.write_bytes(b"fixture")
        env = recorder.isolated_environment(root, runtime)
        config = recorder.write_config(env)
        self.assertEqual(config["directoryMode"], "0o700")
        self.assertEqual(config["fileMode"], "0o600")
        self.assertEqual(recorder.decoded(config["bytesBase64"]), profile.PROFILE["runtime"]["configBytes"].encode())
        with self.assertRaises(FileExistsError):
            recorder.write_config(env)

    def test_unverified_archive_is_rejected_before_tar_parse(self):
        root = self.directory()
        bad = root / "bad.tar.gz"
        bad.write_bytes(b"not-authenticated")
        with patch.dict(os.environ, {"S1_B23_DOLT_ARCHIVE": str(bad)}, clear=False):
            with patch.object(tarfile, "open", side_effect=AssertionError("parsed unverified archive")):
                with self.assertRaisesRegex(ValueError, "identity"):
                    recorder.consume_dolt(root)

    def test_extractor_rejects_block_packet(self):
        packet = {"schema": "plug.s1-b23.ordinary-count-capture.v2", "status": "BLOCK", "profile": profile.PROFILE}
        with self.assertRaises(ValueError):
            extract.validate_packet(packet)

    def test_timeout_and_launch_failure_retain_partial_original_bytes(self):
        root = self.directory()
        stdout, stderr = b"partial\x00\xffstdout", b"partial\x00\xfestderr"
        for error, outcome in ((subprocess.TimeoutExpired(["fixture"], 120, stdout, stderr), "timeout"),
                               (FileNotFoundError("fixture executable absent"), "launch-error")):
            commands, streams = [], []
            with patch.object(subprocess, "run", side_effect=error):
                with self.assertRaises(RuntimeError):
                    recorder.run_process(root / "fixture", [], root, {}, "fixture", commands, streams)
            self.assertEqual(len(streams), 1)
            stream = streams[0]
            self.assertEqual(stream["outcome"], outcome)
            self.assertIsNone(stream["exitCode"])
            for channel, wanted in (("stdout", stdout), ("stderr", stderr)):
                original = wanted if outcome == "timeout" else b""
                self.assertEqual(recorder.decoded(stream[channel + "Base64"]), original)
                self.assertEqual(stream[channel + "Sha256"], recorder.digest(original))
                self.assertEqual(stream[channel + "Bytes"], len(original))

    def synthetic_capture(self, *, stop_verified=True, timeout_init=False):
        root = self.directory("s1-b23-count-capture-")
        binary, runtime = root / "bd", root / "runtime-bin/dolt"
        result = {}
        updated = False

        def fake_run(argv, **kwargs):
            nonlocal updated
            args = argv[1:]
            if argv[0] == str(binary):
                self.assertEqual(args[:len(profile.BD_GLOBAL_ARGS)], profile.BD_GLOBAL_ARGS)
                args = args[len(profile.BD_GLOBAL_ARGS):]
            output = b"{}\n"
            if args == ["init", "-q"]:
                output = b""
            elif args == ["config", "--global", "--get", "versioncheck.disabled"]:
                output = b"true\n"
            elif args == ["version"]:
                output = b"dolt version 2.2.0\n"
            elif args[0] == "init" and timeout_init:
                raise subprocess.TimeoutExpired(argv, 120, b"init partial\xff", b"init diagnostic\xfe")
            elif args[0] == "update":
                updated = True
            elif args[0] == "count":
                value = 4
                if "closed" in args:
                    value = 1
                if "needle" in args:
                    value = 2 if updated else 3
                if "absent" in args:
                    value = 0
                if "changed" in args:
                    value = 1
                body = {"count": value}
                if "--by-status" in args:
                    body = {"total": 4, "groups": [{"group": "open", "count": 4}]}
                output = recorder.canonical(body) + b"\n"
            elif args[:2] == ["dolt", "stop"]:
                output = recorder.canonical({"stopped": stop_verified, "verified": stop_verified}) + b"\n"
            return subprocess.CompletedProcess(argv, 0, output, b"")

        with patch.object(subprocess, "run", side_effect=fake_run):
            recorder.capture_one(binary, runtime, "first", result)
        self.addCleanup(shutil.rmtree, result["workspace"], True)
        return result, str(binary), str(runtime)

    def test_raw_projection_is_derived_even_when_forged_envelope_hashes_match(self):
        result, binary, runtime = self.synthetic_capture()
        extract.validate_result(result, binary, runtime)
        altered = copy.deepcopy(result)
        index = profile.COUNT_STEPS.index("open-task-count")
        raw = b'{"count":99}\n'
        stream = altered["streams"][index]
        stream.update(stdoutBase64=recorder.encoded(raw), stdoutBytes=len(raw), stdoutSha256=recorder.digest(raw))
        altered["commands"][index]["stdout"] = raw.decode()
        altered["rawStreamsSha256"] = recorder.digest(altered["streams"])
        altered["rawJSON"] = recorder.canonical(altered["commands"]).decode()
        altered["rawSha256"] = recorder.digest(altered["rawJSON"].encode())
        altered["normalizedTranscriptSha256"] = extract.normalized_digest(altered["commands"])
        with self.assertRaisesRegex(ValueError, "projection differs from raw"):
            extract.validate_result(altered, binary, runtime)

    def test_argv_environment_and_shutdown_cannot_be_substituted(self):
        result, binary, runtime = self.synthetic_capture()
        for mutate in (
            lambda r: r["streams"][0]["argv"].append("--bare"),
            lambda r: r["environment"].update(HOME="/home/ambient"),
            lambda r: r.update(shutdownVerified=False),
        ):
            altered = copy.deepcopy(result)
            mutate(altered)
            with self.assertRaises(ValueError):
                extract.validate_result(altered, binary, runtime)

    def test_failed_shutdown_retains_workspace_and_timeout_bytes(self):
        result, _, _ = self.synthetic_capture(stop_verified=False, timeout_init=True)
        self.assertIsNotNone(result["failure"])
        self.assertFalse(result["shutdownVerified"])
        self.assertTrue(result["workspaceRetained"])
        self.assertTrue(Path(result["workspace"]).is_dir())
        init = next(item for item in result["streams"] if item["label"] == "init")
        self.assertEqual(init["outcome"], "timeout")
        self.assertEqual(recorder.decoded(init["stdoutBase64"]), b"init partial\xff")
        self.assertEqual(recorder.decoded(init["stderrBase64"]), b"init diagnostic\xfe")
        self.assertEqual(result["streams"][-1]["label"], "cleanup-stop")
        self.assertIn("cleanupError", result)

    def test_setup_failure_returns_a_retained_result(self):
        root = self.directory("s1-b23-count-capture-")
        result = {}
        with patch.object(recorder, "write_config", side_effect=PermissionError("fixture")), patch.object(subprocess, "run", side_effect=AssertionError("runtime must not start")):
            recorder.capture_one(root / "bd", root / "runtime-bin/dolt", "first", result)
        self.addCleanup(shutil.rmtree, result["workspace"], True)
        self.assertEqual(result["failure"]["type"], "PermissionError")
        self.assertEqual(result["streams"], [])
        self.assertTrue(result["workspaceRetained"])

    def test_embedded_module_names_and_command_sequence_are_complete(self):
        directory = Path(__file__).parent
        self.assertEqual(set(profile.MODULES), {path.name for path in directory.glob("*.py")})
        self.assertEqual(profile.COUNT_STEPS[0], "git-init")
        self.assertEqual(len(profile.COMMANDS), 19)


if __name__ == "__main__":
    unittest.main()
