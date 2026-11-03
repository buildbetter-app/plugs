#!/usr/bin/env python3
"""Synthetic policy fixtures only. Never source-engine or raw-capture proof."""

import base64
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import extract_reference_packet as extract
import public_reference_profile as profile
import record_reference as recorder
import reference_capture as capture


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(patch.stopall)
        patch.object(recorder, "WORK_ROOT", self.root / "work").start()

    def frame(self, value):
        payload = extract.canonical(value)
        identity = str(len(payload)).encode() + b" " + extract.digest(payload).encode()
        return profile.BEGIN + b" " + identity + b"\n" + payload + b"\n" + profile.END + b" " + identity + b"\n"

    def source(self):
        source = self.root / "prepared-source"
        source.mkdir()
        env = recorder.isolated_environment(self.root / "fixture-environment", "fixture")

        def git(*args):
            return subprocess.check_output([recorder.GIT, "-C", str(source), *args], env=env)

        git("init", "--quiet", "--template=")
        (source / "sample.go").write_bytes(b"package fixture\n")
        git("add", "sample.go")
        git("-c", "core.logAllRefUpdates=false", "commit", "-qm", "fixture only")
        commit = git("rev-parse", "HEAD").decode().strip()
        tree = git("rev-parse", "HEAD^{tree}").decode().strip()
        manifest = git("ls-tree", "-r", "--full-tree", "HEAD")
        patch.object(recorder, "PIN", commit).start()
        patch.object(recorder, "TREE", tree).start()
        patch.dict(profile.PROFILE, {"source": {
            "commit": commit, "tree": tree, "manifestSha256": extract.digest(manifest), "manifestEntries": 1,
        }}).start()
        return source

    def manifest(self):
        selected = profile.PROFILE
        return {
            "schema": "plug-public-bundle/1",
            "source": {"beads": {
                "commit": selected["source"]["commit"], "tree": selected["source"]["tree"],
                "manifest_sha256": selected["source"]["manifestSha256"],
                "manifest_entries": selected["source"]["manifestEntries"],
            }},
            "artifacts": {"bin/bd": selected["binary"], "bin/plug": {
                "mode": "0o755", "sha256": extract.digest(b"fixture-plug"), "size": len(b"fixture-plug"),
            }},
            "archive": selected["archive"],
            "build": {"run_id": selected["producer"]["run"], "beads": selected["build"]["command"]},
            "toolchain": {"go": selected["build"]["go"]},
            "workflow": {"ref": "refs/heads/" + selected["producer"]["branch"],
                         "sha": selected["producer"]["workflowSha"], "profile": "plug-public-bundle/1"},
        }

    def bundle(self, *, bad_tar_mode=False):
        source = self.source()
        binary = b"fixture-bd-never-executed"
        patch.dict(profile.PROFILE, {"binary": {"mode": "0o755", "sha256": extract.digest(binary), "size": len(binary)}}).start()
        tar_bytes = io.BytesIO()
        with tarfile.open(fileobj=tar_bytes, mode="w") as stream:
            for name, data in (("bin/bd", binary), ("bin/plug", b"fixture-plug")):
                item = tarfile.TarInfo(name)
                item.size, item.mode = len(data), 0o777 if bad_tar_mode else 0o755
                stream.addfile(item, io.BytesIO(data))
        archive = tar_bytes.getvalue()
        patch.dict(profile.PROFILE, {"archive": {"name": "bundle.tar", "sha256": extract.digest(archive), "size": len(archive)}}).start()
        manifest = extract.canonical(self.manifest())
        patch.dict(profile.PROFILE, {"manifestSha256": extract.digest(manifest)}).start()
        zip_path = self.root / "fixture.zip"
        with zipfile.ZipFile(zip_path, "w") as stream:
            for name, data in {"manifest.json": manifest, "receipt.json": manifest,
                               "bundle.tar": archive, "bin/bd": binary, "bin/plug": b"fixture-plug"}.items():
                stream.writestr(name, data)
        patch.dict(profile.PROFILE, {"zip": {"size": zip_path.stat().st_size, "sha256": extract.digest(zip_path.read_bytes())}}).start()
        manifest_path = self.root / "fixture-manifest.json"
        manifest_path.write_bytes(manifest)
        environment = {
            "PLUG_PUBLIC_REFERENCE": profile.PROFILE["descriptor"],
            "PLUG_PUBLIC_REFERENCE_BUILD_RUN": profile.PROFILE["producer"]["run"],
            "PLUG_PUBLIC_REFERENCE_ZIP": str(zip_path), "PLUG_PUBLIC_REFERENCE_SOURCE": str(source),
            "PLUG_PUBLIC_REFERENCE_MANIFEST": str(manifest_path),
        }
        patch.dict(os.environ, environment, clear=True).start()
        destination = self.root / "capture"
        destination.mkdir()
        return destination, zip_path

    def b09_run(self, variation):
        first = {"id": "b09-" + ("abc" if variation == 0 else "xyz"), "title": "Fix login",
                 "description": "Details", "issue_type": "task", "status": "open",
                 "created_at": "2026-11-03T00:00:0" + str(variation) + "Z"}
        second = {**first, "id": "b09-" + ("def" if variation == 0 else "uvw")}
        stdout = {"init": b"initialized\n", "hash-first": json.dumps(first).encode(),
                  "hash-second": json.dumps(second).encode(), "hash-collision-refusal": b"",
                  "hash-list": json.dumps([first, second]).encode()}
        recorded = recorder.Recorder(self.root / "never-executed", self.root, "B09")
        try:
            for label in extract.LABELS["B09"]:
                expected = 1 if label == "hash-collision-refusal" else 0
                proc = subprocess.CompletedProcess([], expected, stdout[label], b"refused\n" if expected else b"")
                with patch.object(recorder.subprocess, "run", return_value=proc):
                    recorded.run(label, ["fixture-only"], expected)
            projection = {"createdIDsDistinct": True, "createdIDPattern": True, "sameSemanticFields": True,
                          "collisionRefused": True, "rowsAfterCollision": 2}
            observations = recorder.normalize({"first": first, "second": second, "rows": [first, second]}, "B09")
            run = {
                "schema": "plug.s1-b09-b12.beads-reference.v2", "card": "B09", "engine": "embedded-dolt",
                "sourceCommit": profile.PROFILE["source"]["commit"], "sourceTree": profile.PROFILE["source"]["tree"],
                "binarySha256": profile.PROFILE["binary"]["sha256"], "sourceTests": profile.SOURCE_TESTS["B09"], "sourceRefs": [],
                "workspaceId": "s1-b09-fixture-" + str(variation), "commands": recorded.commands, "streams": recorded.streams,
                "projection": projection, "observations": observations,
            }
            self.redigest(run)
            return run
        finally:
            shutil.rmtree(recorded.workspace)

    def redigest(self, run):
        run["rawStreamsSha256"] = extract.digest(run["streams"])
        run["normalizedTranscriptSha256"] = extract.digest({"commands": run["commands"], "observations": run["observations"]})
        run["projectionSha256"] = extract.digest(run["projection"])

    def packet(self):
        # A deliberately synthetic, tiny source manifest exercises the complete
        # offline validation path. Patches exist only in this unit-test process.
        files = {}
        for tests in profile.SOURCE_TESTS.values():
            for item in tests:
                path, symbol = item.split("::", 1)
                files[path] = files.get(path, b"package fixture\n") + b"func " + symbol.encode() + b"() {}\n"
        for path in ("internal/storage/schema/migrations/001_fixture.up.sql",
                     "internal/storage/schema/migrations/ignored/002_fixture.up.sql"):
            files[path] = b"-- synthetic migration fixture only\n"
        manifest = b"".join(b"100644 blob " + hashlib_blob(data).encode() + b"\t" + path.encode() + b"\n"
                            for path, data in sorted(files.items()))
        patch.dict(profile.PROFILE, {"source": {
            **profile.PROFILE["source"], "manifestSha256": extract.digest(manifest), "manifestEntries": len(files),
        }}).start()
        source = {
            **profile.PROFILE["source"], "cleanWorkingTree": True, "manifestBase64": capture.encoded(manifest),
            "evidenceBlobs": [{"path": path, "sha256": extract.digest(data), "bytesBase64": capture.encoded(data)}
                              for path, data in sorted(files.items())],
        }
        schema = {}
        for plane, directory, cursor in (("main", "internal/storage/schema/migrations", 1),
                                         ("ignored", "internal/storage/schema/migrations/ignored", 2)):
            items = [{"path": path, "sha256": extract.digest(data)} for path, data in sorted(files.items())
                     if str(Path(path).parent) == directory]
            schema[plane] = {"cursor": cursor, "files": len(items), "digest": extract.digest(items)}
        source["schemaEvidence"] = schema
        refs = extract.validate_source(source)
        public_manifest = extract.canonical(self.manifest())
        patch.dict(profile.PROFILE, {"manifestSha256": extract.digest(public_manifest)}).start()
        patch.object(extract, "CARDS", ("B09",)).start()
        first, repeat = self.b09_run(0), self.b09_run(1)
        for run in (first, repeat):
            run["sourceRefs"] = refs["B09"]
        equality = recorder.compare_runs(first, repeat)
        candidate = {"commit": "a" * 40, "tree": "b" * 40,
                     "modules": {name: "c" * 64 for name in profile.MODULES}}
        candidate["modules"]["record_reference.py"] = profile.PROFILE["reviewedRecorderSha256"]
        configuration = {"inheritAmbient": False, "environment": recorder.ENVIRONMENT,
                         "gitConfiguration": recorder.GIT_CONFIGURATION}
        packet = {
            "schema": profile.PACKET_SCHEMA, "status": "PASS", "candidate": candidate,
            "profile": copy.deepcopy(profile.PROFILE), "profileSha256": extract.digest(profile.PROFILE),
            "qualification": profile.QUALIFICATION, "inProgress": {}, "source": source,
            "configuration": configuration, "configurationSha256": extract.digest(configuration),
            "bundle": {"manifestBase64": capture.encoded(public_manifest), "receiptBase64": capture.encoded(public_manifest),
                       "receiptSha256": extract.digest(public_manifest), "zip": profile.PROFILE["zip"],
                       "binary": profile.PROFILE["binary"], "archive": profile.PROFILE["archive"]},
            "recordings": [{"card": "B09", "first": first, "repeat": repeat, "equality": equality,
                            "disposition": {"status": "keep", "source": "engdocs/design/plug/spec/behaviors/content.md#B09"}}],
        }
        return packet, copy.deepcopy(candidate)

    def test_complete_frame_retains_exact_bytes(self):
        packet = {"fixtureOnly": "\u00e9", "streams": ["/wANCg=="]}
        decoded, raw = extract.unframe(b"unrelated log\n" + self.frame(packet) + b"PASS\n")
        self.assertEqual(decoded, packet)
        self.assertEqual(raw, extract.canonical(packet))

    def test_missing_truncated_duplicate_changed_and_crlf_frames_block(self):
        good = self.frame({"fixtureOnly": True})
        variants = [b"", good[:-1], good[:-20], good + good, good.replace(b"true", b"null"), good.replace(b"\n", b"\r\n"),
                    good.replace(profile.END, b"OTHER_END"), good.replace(b"true", b" true")]
        for invalid in variants:
            with self.subTest(invalid=invalid[:40]), self.assertRaises(ValueError):
                extract.unframe(invalid)

    def test_wrong_descriptor_and_missing_inputs_never_execute(self):
        root = self.root / "capture"
        root.mkdir()
        with patch.dict(os.environ, {"PLUG_PUBLIC_REFERENCE": "untrusted"}, clear=True):
            with patch.object(capture, "snapshot_source", side_effect=AssertionError("reached source")):
                with self.assertRaisesRegex(ValueError, "descriptor"):
                    capture.consume_bundle(root)

    def test_zip_stripped_mode_reconstructed_only_after_both_validators(self):
        root, _ = self.bundle()
        binary, source, manifest, metadata = capture.consume_bundle(root)
        self.assertEqual(binary.read_bytes(), b"fixture-bd-never-executed")
        self.assertEqual(binary.stat().st_mode & 0o777, 0o755)
        self.assertNotEqual(source, Path(os.environ["PLUG_PUBLIC_REFERENCE_SOURCE"]))
        self.assertEqual(extract.digest(manifest), profile.PROFILE["source"]["manifestSha256"])
        self.assertEqual(metadata["binary"], profile.PROFILE["binary"])

    def test_bad_tar_mode_refuses_before_execute_permission(self):
        root, _ = self.bundle(bad_tar_mode=True)
        with self.assertRaisesRegex(ValueError, "type/mode/size"):
            capture.consume_bundle(root)
        self.assertFalse((root / "bd").exists())

    def test_altered_zip_has_no_source_or_binary_fallback(self):
        root, archive = self.bundle()
        archive.write_bytes(b"tampered fixture")
        with patch.object(capture, "snapshot_source", side_effect=AssertionError("reached source")):
            with self.assertRaisesRegex(ValueError, "ZIP size"):
                capture.consume_bundle(root)
        self.assertFalse((root / "bd").exists())

    def test_dirty_source_and_external_git_state_never_grant_execute(self):
        for kind in ("dirty", "external"):
            with self.subTest(kind=kind):
                source = self.source()
                if kind == "dirty":
                    (source / "sample.go").write_bytes(b"changed fixture\n")
                else:
                    (source / ".git/commondir").write_text("/untrusted\n")
                root = self.root / ("capture-" + kind)
                root.mkdir()
                with self.assertRaises((ValueError, RuntimeError)):
                    capture.snapshot_source(source, root)
                shutil.rmtree(source)

    def test_source_is_a_private_byte_snapshot_after_original_replacement(self):
        source = self.source()
        root = self.root / "capture"
        root.mkdir()
        private, _ = capture.snapshot_source(source, root)
        (source / "sample.go").write_bytes(b"mutated after validation\n")
        self.assertEqual((private / "sample.go").read_bytes(), b"package fixture\n")
        self.assertNotEqual((private / "sample.go").stat().st_ino, (source / "sample.go").stat().st_ino)

    def test_raw_inequality_and_normalized_equality_are_measured_separately(self):
        first, repeat = self.b09_run(0), self.b09_run(1)
        left, right = extract.validate_run(first, "B09", []), extract.validate_run(repeat, "B09", [])
        equality = extract.measured_equality(first, repeat, left, right)
        self.assertTrue(equality["normalizedTranscriptsEqual"])
        self.assertTrue(equality["projectionsEqual"])
        self.assertFalse(equality["rawStreamsEqual"])
        self.assertEqual(equality, recorder.compare_runs(first, repeat))

    def test_stream_digest_and_forged_normalization_are_rejected(self):
        for change in ("digest", "normalization", "projection", "exit"):
            with self.subTest(change=change):
                run = self.b09_run(0)
                if change == "digest":
                    run["streams"][0]["stdoutSha256"] = "0" * 64
                elif change == "normalization":
                    run["commands"][0]["stdout"] = "invented output"
                elif change == "projection":
                    run["projection"]["rowsAfterCollision"] = 99
                else:
                    run["commands"][0]["exitCode"] = 1
                self.redigest(run)
                with self.assertRaises(ValueError):
                    extract.validate_run(run, "B09", [])

    def test_unexpected_exit_and_timeout_keep_actual_streams(self):
        recorded = capture.LosslessRecorder(self.root / "never-executed", self.root, "B09")
        self.addCleanup(shutil.rmtree, recorded.workspace, True)
        result = subprocess.CompletedProcess([], 9, b"actual\xff\r\n", b"failed\x00")
        with patch.object(recorder.subprocess, "run", return_value=result):
            with self.assertRaises(RuntimeError):
                recorded.run("unexpected", ["fixture-only"])
        timeout = subprocess.TimeoutExpired("fixture-only", 120, output=b"partial\xfe", stderr=b"timeout\r\n")
        with patch.object(recorder.subprocess, "run", side_effect=timeout):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                recorded.run("timeout", ["fixture-only"])
        self.assertEqual(extract.decode(recorded.streams[0]["stdoutBase64"]), result.stdout)
        self.assertEqual(extract.decode(recorded.streams[1]["stdoutBase64"]), b"partial\xfe")
        self.assertIsNone(recorded.commands[1]["exitCode"])
        self.assertTrue(recorded.commands[1]["timedOut"])

    def test_legacy_binary_and_reviewed_recorder_remain_distinct(self):
        self.assertEqual(recorder.BINARY_SHA256, profile.PROFILE["legacyBinarySha256"])
        self.assertNotEqual(recorder.BINARY_SHA256, profile.PROFILE["binary"]["sha256"])
        self.assertEqual(extract.digest(Path(recorder.__file__).read_bytes()), profile.PROFILE["reviewedRecorderSha256"])

    def test_complete_fixture_packet_validates_without_claiming_engine_proof(self):
        packet, candidate = self.packet()
        decoded, _ = extract.unframe(self.frame(packet))
        extract.validate_packet(decoded, candidate)
        self.assertFalse(decoded["recordings"][0]["equality"]["rawStreamsEqual"])

    def test_forged_profile_source_equality_config_or_candidate_blocks(self):
        original, candidate = self.packet()
        for change in ("profile", "source", "blob", "equality", "config", "candidate", "status"):
            with self.subTest(change=change):
                packet = copy.deepcopy(original)
                if change == "profile":
                    packet["profile"]["canonicalBazel"] = True
                    packet["profileSha256"] = extract.digest(packet["profile"])
                elif change == "source":
                    packet["source"]["manifestBase64"] = capture.encoded(b"invented manifest\n")
                elif change == "blob":
                    item = packet["source"]["evidenceBlobs"][0]
                    item["bytesBase64"], item["sha256"] = capture.encoded(b"altered"), extract.digest(b"altered")
                elif change == "equality":
                    packet["recordings"][0]["equality"]["rawStreamsEqual"] = True
                elif change == "config":
                    packet["configuration"]["inheritAmbient"] = True
                    packet["configurationSha256"] = extract.digest(packet["configuration"])
                elif change == "candidate":
                    packet["candidate"]["tree"] = "0" * 40
                else:
                    packet["status"] = "BLOCK"
                with self.assertRaises(ValueError):
                    extract.validate_packet(packet, candidate)

    def test_b12_exact_refusals_remain_an_unsupported_boundary(self):
        commands = {
            "admin-compact-apply": {"exitCode": 1, "stderr": "Error: 'bd admin compact' is not yet supported in embedded mode\n"},
            "restore-read": {"exitCode": 1, "stderr": "Error: issue b12-source is not compacted\nHint: only compacted issues need restoration\n"},
        }
        commands["restore-apply"] = copy.deepcopy(commands["restore-read"])
        projection, _ = extract.projected("B12", {"admin-compact-analyze": {"summary": {"total_candidates": 0}}}, commands)
        self.assertTrue(projection["applyRefusedInEmbeddedMode"])
        commands["restore-apply"]["exitCode"] = 0
        with self.assertRaisesRegex(ValueError, "unsupported"):
            extract.projected("B12", {"admin-compact-analyze": {"summary": {"total_candidates": 0}}}, commands)


def hashlib_blob(data):
    import hashlib
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


if __name__ == "__main__":
    unittest.main(verbosity=2)
