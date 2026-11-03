#!/usr/bin/env python3
"""Labelled synthetic policy fixtures; never reference/runtime acceptance bytes."""

import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import unittest
from unittest.mock import patch

import extract_reference_packet as common
import extract_proxied_reference_packet as extract
import proxied_reference_profile as profile
import record_reference as recorder
import reference_capture as original
import reference_capture_policy_test as fixtures
import reference_proxied_capture as capture


RUNTIME_FIXTURE_SHA = common.digest(b"fixture-dolt-never-executed")


def configuration_fixture(home):
    data = profile.RUNTIME_CONFIGURATION["contents"].encode()
    return {
        "operation": profile.RUNTIME_CONFIGURATION["operation"],
        "rootPath": home, "path": home + "/" + profile.RUNTIME_CONFIGURATION["relativePath"],
        "relativePath": profile.RUNTIME_CONFIGURATION["relativePath"],
        "directoryMode": "0o700", "fileMode": "0o600",
        "bytesBase64": original.encoded(data), "sha256": common.digest(data),
        "environment": {"DOLT_ROOT_PATH": home, "DOLT_DISABLE_EVENT_FLUSH": "1"},
    }


def probe_fixture(args, stdout, code=0, stderr=b""):
    return {
        "argv": ["verified-runtime", *args], "exitCode": code,
        "stdoutBase64": original.encoded(stdout), "stderrBase64": original.encoded(stderr),
        "stdoutSha256": common.digest(stdout), "stderrSha256": common.digest(stderr),
    }


class ProxiedPolicies(fixtures.PolicyTests):
    # The retained parent policy cases still run against the unchanged embedded
    # modules. The following additional cases use explicitly synthetic bytes.
    def proxied_run(self, variation):
        run = self.b09_run(variation)
        run.update({"schema": profile.RUN_SCHEMA, "engine": profile.ENGINE,
                    "runtimeBinarySha256": RUNTIME_FIXTURE_SHA, "shutdownVerified": True})
        run["runtimeConfiguration"] = configuration_fixture(
            "/tmp/plug-swarm/work/" + run["workspaceId"] + "/.recorder-env/home")
        run["commands"][0]["argv"] = ["reference-engine", *profile.RUNTIME["init"], "--prefix", "b09", "--quiet"]
        shutdown = {"stopped": True, "force": False, "verified": True}
        stdout = json.dumps(shutdown).encode()
        run["commands"].append({"label": "cleanup-stop", "argv": ["reference-engine", *profile.RUNTIME["shutdown"]],
                                "exitCode": 0, "stdout": shutdown, "stderr": ""})
        run["streams"].append({"label": "cleanup-stop", "stdoutBase64": original.encoded(stdout),
                               "stderrBase64": "", "stdoutSha256": common.digest(stdout), "stderrSha256": common.digest(b"")})
        self.redigest(run)
        return run

    def proxied_packet(self):
        packet, _ = self.packet()
        source = packet["source"]
        lines = {line.split(b"\t", 1)[1].decode(): line for line in common.decode(source["manifestBase64"]).splitlines()}
        proofs, checksums = [], {}
        for path in profile.RUNTIME_SOURCE:
            data = ("synthetic runtime source fixture only: " + path).encode()
            checksums[path] = common.digest(data)
            proofs.append({"path": path, "sha256": checksums[path], "bytesBase64": original.encoded(data)})
            lines[path] = b"100644 blob " + fixtures.hashlib_blob(data).encode() + b"\t" + path.encode()
        patch.dict(profile.RUNTIME_SOURCE, checksums, clear=True).start()
        manifest = b"".join(lines[path] + b"\n" for path in sorted(lines))
        patch.dict(profile.PUBLIC_BUILD, {"source": {**profile.PUBLIC_BUILD["source"],
                   "manifestSha256": common.digest(manifest), "manifestEntries": len(lines)}}).start()
        source.update({**profile.PUBLIC_BUILD["source"], "manifestBase64": original.encoded(manifest)})
        public_manifest = common.canonical(self.manifest())
        patch.dict(profile.PUBLIC_BUILD, {"manifestSha256": common.digest(public_manifest)}).start()
        packet["bundle"].update({"manifestBase64": original.encoded(public_manifest), "receiptBase64": original.encoded(public_manifest),
                                 "receiptSha256": common.digest(public_manifest)})
        first, repeat = self.proxied_run(0), self.proxied_run(1)
        refs = common.validate_source(source)
        for run in (first, repeat):
            run["sourceRefs"] = refs["B09"]
        probe = probe_fixture(["version"], profile.RUNTIME["versionStdout"].encode())
        runtime = {"mode": profile.RUNTIME["mode"], "version": profile.RUNTIME["version"], "versionProbe": probe,
                   "configuration": configuration_fixture("/tmp/plug-swarm/work/s1-proxied-capture-fixture/version-environment/home"),
                   "configurationProbe": probe_fixture(profile.RUNTIME["configurationProbe"], b"true\n"),
                   "archive": {"sha256": profile.RUNTIME["archiveSha256"], "bytes": 123, "url": profile.RUNTIME["url"]},
                   "binary": {"sha256": RUNTIME_FIXTURE_SHA, "bytes": 456, "mode": "0o700", "member": profile.RUNTIME["member"]}}
        candidate = {"commit": "a" * 40, "tree": "b" * 40, "modules": {name: "c" * 64 for name in profile.MODULES}}
        candidate["modules"]["record_reference.py"] = profile.PUBLIC_BUILD["reviewedRecorderSha256"]
        packet["configuration"]["runtimePath"] = "verified private runtime-bin prepended to /usr/bin:/bin"
        packet["configuration"]["runtimeConfiguration"] = profile.RUNTIME_CONFIGURATION
        packet.update({"schema": profile.PACKET_SCHEMA, "profile": copy.deepcopy(profile.PROFILE),
                       "profileSha256": common.digest(profile.PROFILE), "qualification": profile.QUALIFICATION,
                       "candidate": candidate, "runtime": runtime, "runtimeSource": proofs,
                       "configurationSha256": common.digest(packet["configuration"]),
                       "selectedCards": ["B09"], "unexecuted": profile.UNEXECUTED,
                       "recordings": [{"card": "B09", "first": first, "repeat": repeat,
                                       "equality": recorder.compare_runs(first, repeat),
                                       "disposition": {"status": "keep", "source": "engdocs/design/plug/spec/behaviors/content.md#B09"}}]})
        return packet, copy.deepcopy(candidate)

    def runtime_archive(self, members):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w:gz") as stream:
            for kind in members:
                item = tarfile.TarInfo(profile.RUNTIME["member"])
                item.mode = 0o755
                if kind == "regular":
                    value = b"synthetic runtime; never executed"
                    item.size = len(value)
                    stream.addfile(item, io.BytesIO(value))
                else:
                    item.type, item.linkname = tarfile.SYMTYPE, "/untrusted"
                    stream.addfile(item)
        path = self.root / "fixture-runtime.tar.gz"
        path.write_bytes(data.getvalue())
        patch.dict(os.environ, {"S1_B09_B12_DOLT_ARCHIVE": str(path)}, clear=True).start()
        return data.getvalue()

    def test_proxied_archive_mismatch_never_parses_or_executes(self):
        self.runtime_archive(["regular"])
        with patch.object(capture.tarfile, "open", side_effect=AssertionError("parsed unverified archive")), \
                patch.object(capture.subprocess, "run", side_effect=AssertionError("executed unverified runtime")):
            with self.assertRaisesRegex(ValueError, "digest"):
                capture.consume_runtime(self.root, {})
        self.assertFalse((self.root / "runtime-bin").exists())

    def test_proxied_archive_rejects_duplicate_and_link_member(self):
        for members in (["regular", "regular"], ["link"]):
            with self.subTest(members=members):
                data = self.runtime_archive(members)
                patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
                with self.assertRaisesRegex(ValueError, "duplicate, nonregular"):
                    capture.consume_runtime(self.root, {})
                self.assertFalse((self.root / "runtime-bin").exists())

    def test_proxied_runtime_version_retains_exact_bytes_and_isolated_env(self):
        data = self.runtime_archive(["regular"])
        patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
        result = subprocess.CompletedProcess([], 0, profile.RUNTIME["versionStdout"].encode(), b"")
        progress = {}

        def configured_before_execution(argv, **kwargs):
            env = kwargs["env"]
            home = Path(env["HOME"])
            self.assertEqual(env["DOLT_ROOT_PATH"], str(home))
            self.assertEqual(env["DOLT_DISABLE_EVENT_FLUSH"], "1")
            self.assertEqual((home / ".dolt/config_global.json").read_bytes(), profile.RUNTIME_CONFIGURATION["contents"].encode())
            self.assertEqual((home / ".dolt").stat().st_mode & 0o777, 0o700)
            self.assertEqual((home / ".dolt/config_global.json").stat().st_mode & 0o777, 0o600)
            if argv[1:] == profile.RUNTIME["configurationProbe"]:
                return subprocess.CompletedProcess([], 0, b"true\n", b"")
            self.assertEqual(argv[1:], ["version"])
            return result

        with patch.object(capture.subprocess, "run", side_effect=configured_before_execution) as run:
            binary = capture.consume_runtime(self.root, progress)
        self.assertEqual([call.args[0][1:] for call in run.call_args_list], [profile.RUNTIME["configurationProbe"], ["version"]])
        self.assertEqual(binary.stat().st_mode & 0o777, 0o700)
        self.assertEqual(common.decode(progress["versionProbe"]["stdoutBase64"]), result.stdout)
        self.assertEqual(progress["binary"]["sha256"], common.digest(binary.read_bytes()))
        self.assertNotIn("S1_B09_B12_DOLT_ARCHIVE", run.call_args.kwargs["env"])
        self.assertEqual(run.call_args.kwargs["env"]["PATH"], "/usr/bin:/bin")
        extract.validate_runtime_configuration(progress["configuration"], run.call_args.kwargs["env"]["HOME"])

    def test_proxied_environment_excludes_ambient_runtime_selection(self):
        with patch.dict(os.environ, {"BEADS_DOLT_BIN": "/ambient", "DOLT_ROOT_PATH": "/ambient", "GH_TOKEN": "fixture"}):
            value = capture.ProxiedRecorder(self.root / "no-product", self.root, "B09", self.root / "owned-bin/dolt")
        self.addCleanup(shutil.rmtree, value.workspace, True)
        self.assertEqual(value.env["PATH"], str(self.root / "owned-bin") + ":/usr/bin:/bin")
        self.assertFalse(set(value.env) & {"BEADS_DOLT_BIN", "GH_TOKEN"})
        self.assertEqual(value.env["DOLT_ROOT_PATH"], value.env["HOME"])
        self.assertNotEqual(value.env["DOLT_ROOT_PATH"], "/ambient")
        self.assertEqual(value.env["DOLT_DISABLE_EVENT_FLUSH"], "1")
        extract.validate_runtime_configuration(value.runtime_configuration, value.env["HOME"])
        with self.assertRaisesRegex(ValueError, "selected"):
            capture.ProxiedRecorder(self.root / "no-product", self.root, "B12", self.root / "owned-bin/dolt")

    def test_runtime_probe_timeout_preserves_partial_streams_before_block(self):
        data = self.runtime_archive(["regular"])
        patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
        timeout = subprocess.TimeoutExpired("negative-policy-probe", 30, output=b"partial\xff\r\n", stderr=b"timeout\x00")
        progress = {}
        with patch.object(capture.subprocess, "run", side_effect=[subprocess.CompletedProcess([], 0, b"true\n", b""), timeout]):
            with self.assertRaisesRegex(ValueError, "probe failed"):
                capture.consume_runtime(self.root, progress)
        self.assertIsNone(progress["versionProbe"]["exitCode"])
        self.assertEqual(common.decode(progress["versionProbe"]["stdoutBase64"]), b"partial\xff\r\n")
        self.assertEqual(common.decode(progress["versionProbe"]["stderrBase64"]), b"timeout\x00")

    def test_zero_exit_version_warning_stays_lossless_block(self):
        data = self.runtime_archive(["regular"])
        patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
        # Labelled fixture transcribes the original e71 warning; it is not a
        # replacement capture or accepted runtime output.
        warning = (b"dolt version 2.2.0\nWarning: you are on an old version of Dolt. The newest version is 2.6.2.\n"
                   b"To disable this warning, run 'dolt config --global --add versioncheck.disabled true'\n")
        progress = {}
        with patch.object(capture.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 0, b"true\n", b""),
                subprocess.CompletedProcess([], 0, warning, b"")]):
            with self.assertRaisesRegex(ValueError, "version probe failed"):
                capture.consume_runtime(self.root, progress)
        self.assertEqual(common.decode(progress["versionProbe"]["stdoutBase64"]), warning)
        self.assertEqual(progress["versionProbe"]["stdoutSha256"], common.digest(warning))
        with self.assertRaisesRegex(ValueError, "version"):
            extract.validate_probe(progress["versionProbe"], ["version"], profile.RUNTIME["versionStdout"], "version mismatch")

    def test_configuration_failure_or_timeout_retains_streams_and_never_probes_version(self):
        for timed_out in (False, True):
            with self.subTest(timed_out=timed_out):
                directory = self.root / str(timed_out)
                directory.mkdir()
                data = self.runtime_archive(["regular"])
                patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
                failure = (subprocess.TimeoutExpired("fixture-only", 30, output=b"partial\xff", stderr=b"timeout\x00")
                           if timed_out else subprocess.CompletedProcess([], 8, b"partial\xff", b"timeout\x00"))
                progress = {}
                with patch.object(capture.subprocess, "run", side_effect=[failure]) as run:
                    with self.assertRaisesRegex(ValueError, "configuration probe failed"):
                        capture.consume_runtime(directory, progress)
                self.assertEqual(run.call_count, 1)
                self.assertNotIn("versionProbe", progress)
                self.assertEqual(common.decode(progress["configurationProbe"]["stdoutBase64"]), b"partial\xff")
                self.assertEqual(common.decode(progress["configurationProbe"]["stderrBase64"]), b"timeout\x00")
                self.assertEqual(progress["configurationProbe"]["exitCode"], None if timed_out else 8)
                extract.validate_runtime_configuration(progress["configuration"],
                                                       run.call_args.kwargs["env"]["HOME"])

    def test_preexisting_private_configuration_is_not_reused_or_executed(self):
        data = self.runtime_archive(["regular"])
        patch.dict(profile.RUNTIME, {"archiveSha256": common.digest(data)}).start()
        directory = self.root / "version-environment/home/.dolt"
        directory.mkdir(parents=True)
        (directory / "config_global.json").write_bytes(b'{"versioncheck.disabled":"false"}\n')
        with patch.object(capture.subprocess, "run", side_effect=AssertionError("executed existing config")):
            with self.assertRaises(FileExistsError):
                capture.consume_runtime(self.root, {})
        self.assertEqual((directory / "config_global.json").read_bytes(), b'{"versioncheck.disabled":"false"}\n')

    def test_configuration_mutations_and_incomplete_packet_are_rejected(self):
        packet, candidate = self.proxied_packet()
        with patch.object(extract, "CARDS", ("B09",)):
            extract.validate_packet(packet, candidate)
            for mutation in ("bytes", "mode", "root", "environment", "probe", "run", "incomplete"):
                bad = copy.deepcopy(packet)
                config = bad["runtime"]["configuration"]
                if mutation == "bytes":
                    config["bytesBase64"] = original.encoded(b'{"versioncheck.disabled":"false"}\n')
                    config["sha256"] = common.digest(common.decode(config["bytesBase64"]))
                elif mutation == "mode":
                    config["fileMode"] = "0o644"
                elif mutation == "root":
                    config["rootPath"] = "/ambient/home"
                elif mutation == "environment":
                    config["environment"]["DOLT_ROOT_PATH"] = "/ambient/home"
                elif mutation == "probe":
                    bad["runtime"]["configurationProbe"] = probe_fixture(profile.RUNTIME["configurationProbe"], b"false\n")
                elif mutation == "run":
                    bad["recordings"][0]["first"]["runtimeConfiguration"]["sha256"] = "0" * 64
                else:
                    del bad["runtime"]["configurationProbe"]
                with self.assertRaises((ValueError, KeyError)):
                    extract.validate_packet(bad, candidate)
            packet["status"] = "BLOCK"
            with self.assertRaisesRegex(ValueError, "BLOCK"):
                extract.validate_packet(packet, candidate)

    def test_failed_selection_keeps_raw_streams_and_shutdown_outcome(self):
        for shutdown_ok in (True, False):
            with self.subTest(shutdown_ok=shutdown_ok):
                selected = subprocess.CompletedProcess([], 7, b"actual\x00\xff\r\n", b"refused\r\n")
                stopped = subprocess.CompletedProcess([], 0 if shutdown_ok else 8,
                    b'{"stopped":true,"verified":true}' if shutdown_ok else b"", b"" if shutdown_ok else b"stop refused\x00")
                progress = {}

                def failure(value):
                    value.run("negative-policy-probe", ["fixture-only"])

                with patch.object(capture.ProxiedRecorder, "selection", failure), \
                        patch.object(recorder.subprocess, "run", side_effect=[selected, stopped]):
                    with self.assertRaisesRegex(RuntimeError, "exit 7"):
                        capture.capture_one("B09", self.root / "no-product", self.root, self.root / "owned/dolt", progress)
                self.assertEqual(common.decode(progress["streams"][0]["stdoutBase64"]), selected.stdout)
                self.assertEqual(progress["commands"][-1]["argv"], ["reference-engine", "dolt", "stop", "--json"])
                self.assertEqual(progress["shutdownVerified"], shutdown_ok)
                workspace = recorder.WORK_ROOT / progress["workspaceId"]
                self.assertEqual(workspace.exists(), not shutdown_ok)
                if not shutdown_ok:
                    self.assertIn("cleanupError", progress)
                    self.assertEqual(common.decode(progress["streams"][-1]["stderrBase64"]), b"stop refused\x00")
                    shutil.rmtree(workspace)  # Labelled mocks spawned no process.

    def test_complete_proxied_fixture_preserves_raw_inequality(self):
        packet, candidate = self.proxied_packet()
        # A one-card synthetic fixture exercises deep validation; the production
        # card-set gate is separately tested with the immutable three-card set.
        with patch.object(extract, "CARDS", ("B09",)):
            extract.validate_packet(packet, candidate)
        self.assertFalse(packet["recordings"][0]["equality"]["rawStreamsEqual"])
        with self.assertRaises(ValueError):
            common.validate_packet(packet, candidate)

    def test_successful_stop_exit_without_verified_shutdown_retains_workspace(self):
        progress = {}
        stopped = subprocess.CompletedProcess([], 0, b'{"stopped":true,"verified":false}', b"")
        with patch.object(capture.ProxiedRecorder, "selection", return_value={}), \
                patch.object(recorder.subprocess, "run", return_value=stopped):
            with self.assertRaisesRegex(ValueError, "not verified"):
                capture.capture_one("B09", self.root / "no-product", self.root, self.root / "owned/dolt", progress)
        workspace = recorder.WORK_ROOT / progress["workspaceId"]
        self.assertTrue(workspace.exists())
        self.assertFalse(progress["shutdownVerified"])
        self.assertEqual(common.decode(progress["streams"][-1]["stdoutBase64"]), stopped.stdout)
        shutil.rmtree(workspace)  # Labelled mocks spawned no process.

    def test_proxied_card_set_rejects_missing_extra_duplicate_or_reordered(self):
        good = {"selectedCards": list(profile.CARDS), "unexecuted": profile.UNEXECUTED,
                "recordings": [{"card": card} for card in profile.CARDS]}
        extract.validate_card_set(good)
        for cards in (["B09", "B11"], ["B09", "B10", "B11", "B12"], ["B09", "B10", "B10"], ["B11", "B10", "B09"]):
            invalid = {**good, "recordings": [{"card": card} for card in cards]}
            with self.assertRaisesRegex(ValueError, "card"):
                extract.validate_card_set(invalid)
        with self.assertRaises(ValueError):
            extract.validate_card_set({**good, "unexecuted": {}})

    def test_proxied_validator_rejects_embedded_mode_missing_stop_and_wrong_runtime(self):
        for mutation in ("engine", "init", "stop", "runtime"):
            run = self.proxied_run(0)
            if mutation == "engine":
                run["engine"] = "embedded-dolt"
            elif mutation == "init":
                run["commands"][0]["argv"].remove("--proxied-server")
            elif mutation == "stop":
                run["commands"].pop()
                run["streams"].pop()
            else:
                run["runtimeBinarySha256"] = "0" * 64
            self.redigest(run)
            with self.assertRaises(ValueError):
                extract.validate_run(run, "B09", [], RUNTIME_FIXTURE_SHA)

    def test_proxied_runtime_source_and_probe_mutations_block(self):
        packet, candidate = self.proxied_packet()
        with patch.object(extract, "CARDS", ("B09",)):
            extract.validate_packet(packet, candidate)
            for mutation in ("archive", "probe", "source", "pair", "config"):
                bad = copy.deepcopy(packet)
                if mutation == "archive":
                    bad["runtime"]["archive"]["sha256"] = "0" * 64
                elif mutation == "probe":
                    bad["runtime"]["versionProbe"]["stdoutBase64"] = original.encoded(b"invented version\n")
                elif mutation == "source":
                    bad["runtimeSource"][0]["bytesBase64"] = original.encoded(b"invented source")
                elif mutation == "pair":
                    bad["recordings"][0]["equality"]["rawStreamsEqual"] = True
                else:
                    bad["configuration"]["environment"]["BEADS_DOLT_BIN"] = "/invented/runtime"
                    bad["configurationSha256"] = common.digest(bad["configuration"])
                with self.assertRaises(ValueError):
                    extract.validate_packet(bad, candidate)


if __name__ == "__main__":
    unittest.main(verbosity=2)
