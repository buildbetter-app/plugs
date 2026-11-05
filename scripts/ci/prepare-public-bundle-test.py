#!/usr/bin/env python3
"""Adversarial fixture tests; these do not certify actual binary consumption."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile


spec = importlib.util.spec_from_file_location("prepare_public_bundle", Path(__file__).with_name("prepare-public-bundle.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


class PublicBundleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="public-artifact-prepare-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.index = 0
        self.source_payloads = {"beads": b"public beads source manifest\n", "plug": b"public plug source manifest\n"}
        sources = {
            name: (pin[0], pin[1], hashlib.sha256(self.source_payloads[name]).hexdigest())
            for name, pin in helper.SOURCES.items()
        }
        self.source_patch = patch.dict(helper.SOURCES, sources)
        self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.run = {
            "id": 123, "status": "completed", "conclusion": "success", "event": "workflow_dispatch",
            "path": helper.WORKFLOW, "head_sha": "a" * 40, "head_branch": "public-source",
            "repository": {"full_name": helper.REPOSITORY}, "head_repository": {"full_name": helper.REPOSITORY},
        }
        self.payloads = {"bin/bd-export": b"fixture exporter bytes\n", "bin/roundtrip": b"fixture roundtrip bytes\n"}
        self.manifest = {
            "schema": helper.PROFILE, "targets": copy.deepcopy(helper.TARGETS),
            "source_refs": copy.deepcopy(helper.SOURCE_REFS),
            "workflow": {"ref": "refs/heads/public-source", "sha": "a" * 40, "profile": helper.PROFILE},
            "source": {
                name: {"commit": pin[0], "tree": pin[1], "manifest_sha256": pin[2], "manifest_entries": 1}
                for name, pin in helper.SOURCES.items()
            },
            "build": {"run_id": "123"},
            "resolution": {"bd_export_version": {"commit": helper.BEADS_COMMIT, "build": "public-" + helper.BEADS_COMMIT}},
            "artifacts": {
                name: {"sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload), "mode": "0o755"}
                for name, payload in self.payloads.items()
            },
        }
        self.artifact = {
            "id": 456, "expired": False, "workflow_run": {"id": 123, "head_sha": "a" * 40},
            "name": "plug-public-bundle-test-123", "size_in_bytes": 1234, "digest": "sha256:" + "b" * 64,
        }

    def fixture(self, mutate_manifest=None, mutate_tar=None, mutate_zip=None):
        manifest = copy.deepcopy(self.manifest)
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w") as stream:
            for name, payload in self.payloads.items():
                member = tarfile.TarInfo(name)
                member.mode, member.size = 0o755, len(payload)
                if mutate_tar is not None:
                    mutate_tar(member)
                if not member.isreg():
                    member.size = 0
                stream.addfile(member, io.BytesIO(payload) if member.isreg() else None)
        canonical = archive_buffer.getvalue()
        manifest["archive"] = {"name": "bundle.tar", "sha256": hashlib.sha256(canonical).hexdigest(), "size": len(canonical)}
        if mutate_manifest is not None:
            mutate_manifest(manifest)
        manifest_bytes = json.dumps(manifest).encode()
        files = {
            **self.payloads, "manifest.json": manifest_bytes, "receipt.json": manifest_bytes,
            "bundle.tar": canonical, **{f"source/{name}.manifest": payload for name, payload in self.source_payloads.items()},
        }
        if mutate_zip is not None:
            mutate_zip(files)
        self.index += 1
        archive = self.root / f"artifact-{self.index}.zip"
        with zipfile.ZipFile(archive, "w") as stream:
            for name, payload in files.items():
                stream.writestr(name, payload)
        return archive, self.root / f"output-{self.index}", hashlib.sha256(manifest_bytes).hexdigest()

    def check_fixture(self, **kwargs):
        archive, output, manifest_hash = self.fixture(**kwargs)
        return helper.prepare_archive(archive, output, manifest_hash, self.run, "123"), output

    def test_opt_in_and_duplicate_or_malformed_marker(self):
        descriptor = "123:456:" + "a" * 64
        self.assertEqual(helper.descriptor_from_args("test //..."), "")
        self.assertEqual(helper.descriptor_from_args("test //... " + helper.MARKER + descriptor), descriptor)
        for args in (
            helper.MARKER + descriptor + " " + helper.MARKER + descriptor,
            helper.MARKER + "../123:456:" + "a" * 64,
            helper.MARKER + "123:456:" + "A" * 64,
            "--test_env=PLUG_PUBLIC_BUNDLE " + descriptor,
            helper.MARKER + descriptor + ";echo",
            "--test_env=PLUG_PUBLIC_BUNDLE",
            "build //... " + helper.MARKER + descriptor,
            "test //... " + helper.MARKER + descriptor + " --test_env=PLUG_PUBLIC_BEADS_BINARY=/unverified",
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                helper.descriptor_from_args(args)

    def test_metadata_requires_success_fixed_repository_workflow_and_artifact(self):
        helper.validate_metadata(self.run, self.artifact, "123", "456")
        for field, value in (("conclusion", "failure"), ("status", "in_progress"), ("path", ".github/workflows/other.yml"), ("event", "pull_request")):
            altered = copy.deepcopy(self.run)
            altered[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                helper.validate_metadata(altered, self.artifact, "123", "456")
        for field, value in (("expired", True), ("id", 789), ("workflow_run", {"id": 999, "head_sha": "a" * 40}), ("digest", "sha256:no")):
            altered = copy.deepcopy(self.artifact)
            altered[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                helper.validate_metadata(self.run, altered, "123", "456")
        altered = copy.deepcopy(self.run)
        altered["head_repository"]["full_name"] = "someone/else"
        with self.assertRaises(ValueError):
            helper.validate_metadata(altered, self.artifact, "123", "456")

    def test_companion_artifact_keeps_the_run_suffix(self):
        helper.validate_metadata(self.run, self.artifact, "123", "456", profile=helper.PROFILE)
        for name in ("plug-public-bundle-test", "plug-public-bundle-test-999"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "immutable producer run"):
                helper.validate_metadata(self.run, {**self.artifact, "name": name}, "123", "456",
                                         profile=helper.PROFILE)

    def test_canonical_extraction_restores_only_verified_executable_modes(self):
        manifest, output = self.check_fixture()
        self.assertEqual(manifest["schema"], helper.PROFILE)
        for name, payload in self.payloads.items():
            self.assertEqual((output / name).read_bytes(), payload)
            self.assertEqual(stat.S_IMODE((output / name).stat().st_mode), 0o755)
        self.assertEqual({str(path.relative_to(output)) for path in output.rglob("*") if path.is_file()}, helper.FILES)

    def test_pinned_manifest_source_profile_ref_target_and_workflow(self):
        mutations = (
            lambda m: m.update(schema="plug-public-bundle/1"),
            lambda m: m["source"]["beads"].update(commit="b" * 40),
            lambda m: m["source"]["beads"].update(tree="b" * 40),
            lambda m: m["source_refs"].update(plug="wrong-ref"),
            lambda m: m["targets"]["beads_exporter"].update(label="//cmd/bd:bd_test"),
            lambda m: m["workflow"].update(sha="b" * 40),
            lambda m: m["build"].update(run_id="999"),
            lambda m: m["resolution"]["bd_export_version"].update(commit="b" * 40),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.check_fixture(mutate_manifest=mutate)

    def test_manifest_checksum_and_source_bytes_are_independent(self):
        archive, output, _ = self.fixture()
        with self.assertRaises(ValueError):
            helper.prepare_archive(archive, output, "0" * 64, self.run, "123")
        with self.assertRaises(ValueError):
            self.check_fixture(mutate_zip=lambda files: files.update({"source/beads.manifest": b"wrong source\n"}))
        with self.assertRaises(ValueError):
            self.check_fixture(mutate_zip=lambda files: files.update({"bin/bd-export": b"wrong binary\n"}))

    def test_tar_refuses_links_traversal_duplicate_members_and_wrong_modes(self):
        for mutate in (
            lambda member: setattr(member, "mode", 0o644),
            lambda member: setattr(member, "mode", 0o4755),
            lambda member: setattr(member, "name", "../escape"),
            lambda member: setattr(member, "name", "bin/bd-export"),
            lambda member: setattr(member, "type", tarfile.SYMTYPE),
            lambda member: setattr(member, "type", tarfile.LNKTYPE),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.check_fixture(mutate_tar=mutate)
        self.assertFalse((self.root / "escape").exists())

    def test_zip_refuses_extra_traversal_and_duplicate_members(self):
        for name in ("../escape", "private-ledger.json", "/absolute"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_zip=lambda files: files.update({name: b"unexpected"}))
        archive, output, manifest_hash = self.fixture()
        with zipfile.ZipFile(archive, "a") as stream:
            stream.writestr("source/plug.manifest", b"duplicate")
        with self.assertRaises(ValueError):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123")

    def test_zip_refuses_symlinks(self):
        archive, output, manifest_hash = self.fixture()
        with zipfile.ZipFile(archive) as stream:
            files = {member.filename: stream.read(member) for member in stream.infolist()}
        with zipfile.ZipFile(archive, "w") as stream:
            for name, payload in files.items():
                member = zipfile.ZipInfo(name)
                member.external_attr = (stat.S_IFLNK | 0o777) << 16 if name == "bin/bd-export" else (stat.S_IFREG | 0o644) << 16
                stream.writestr(member, payload)
        with self.assertRaises(ValueError):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123")

    def test_failure_does_not_publish_executable_environment(self):
        environment = self.root / "github-env"
        with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "gh_json", side_effect=[{**self.run, "conclusion": "failure"}, self.artifact]):
            with self.assertRaises(ValueError):
                helper.prepare("123:456:" + "a" * 64, self.root / "prepared", environment, self.root / "evidence")
        self.assertFalse(environment.exists())
        self.assertFalse((self.root / "prepared").exists())


class NativeBundleTest(unittest.TestCase):
    """Synthetic containers only; no official descriptor, delivery or execution proof."""

    fixture = PublicBundleTest.fixture

    def setUp(self):
        PublicBundleTest.setUp(self)
        self.source_payloads = {"beads": b"fixture-only source entry\n" * helper.NATIVE_SOURCE_ENTRIES}
        self.payloads = {"bin/bd-export": b"fixture-only native executable bytes\n"}
        sources = {name: (pin[0], pin[1], hashlib.sha256(self.source_payloads[name]).hexdigest())
                   for name, pin in helper.NATIVE_SOURCES.items()}
        source_patch = patch.dict(helper.NATIVE_SOURCES, sources)
        source_patch.start()
        self.addCleanup(source_patch.stop)
        self.run.update(workflow_id=403922586, run_attempt=1,
                        actor={"login": "nikhil-s-buildbetter-remote-dev[bot]"},
                        triggering_actor={"login": "nikhil-s-buildbetter-remote-dev[bot]"})
        self.manifest = {
            "schema": helper.NATIVE_PROFILE, "source_refs": copy.deepcopy(helper.NATIVE_SOURCE_REFS),
            "targets": copy.deepcopy(helper.NATIVE_TARGETS),
            "workflow": {"ref": "refs/heads/public-source", "sha": "a" * 40, "profile": helper.NATIVE_PROFILE},
            "source": {name: {"commit": pin[0], "tree": pin[1], "manifest_sha256": pin[2],
                              "manifest_entries": helper.NATIVE_SOURCE_ENTRIES} for name, pin in sources.items()},
            "build": {"run_id": "123", "beads": helper.NATIVE_BUILD},
            "toolchain": copy.deepcopy(helper.NATIVE_TOOLCHAIN),
            "resolution": {
                "bd_export_help_exit": 0,
                "bd_export_version": {"commit": helper.NATIVE_BEADS_COMMIT, "build": "public-" + helper.NATIVE_BEADS_COMMIT},
                **{key: hashlib.sha256(("fixture-only " + key).encode()).hexdigest() for key in
                   ("bd_export_help_sha256", "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256")},
            },
            "artifacts": {name: {"sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload), "mode": "0o755"}
                          for name, payload in self.payloads.items()},
        }

    def metadata(self, archive, manifest_hash):
        artifact = {**self.artifact, "name": "plug-public-bundle-fixture-native",
                    "size_in_bytes": archive.stat().st_size, "digest": "sha256:" + helper.digest(archive)}
        admission = helper.NativeAdmission("123:456:" + manifest_hash, self.run["head_sha"],
                                          self.run["head_branch"], "fixture-native", artifact["digest"],
                                          artifact["size_in_bytes"])
        return artifact, admission

    def check_fixture(self, **kwargs):
        archive, output, manifest_hash = self.fixture(**kwargs)
        artifact, admission = self.metadata(archive, manifest_hash)
        with patch.object(helper, "NATIVE_ADMISSION", admission):
            manifest = helper.prepare_archive(archive, output, manifest_hash, self.run, "123", artifact=artifact)
        return manifest, output

    def test_native_acceptance_has_one_source_and_one_exact_executable(self):
        manifest, output = self.check_fixture()
        self.assertEqual(set(manifest["source"]), {"beads"})
        self.assertEqual(set(manifest["targets"]), {"beads_exporter"})
        self.assertEqual(set(manifest["artifacts"]), {"bin/bd-export"})
        self.assertEqual({str(path.relative_to(output)) for path in output.rglob("*") if path.is_file()}, helper.NATIVE_FILES)
        self.assertEqual((output / "bin/bd-export").read_bytes(), self.payloads["bin/bd-export"])
        self.assertEqual(stat.S_IMODE((output / "bin/bd-export").stat().st_mode), 0o755)
        self.assertFalse((output / "bin/roundtrip").exists())
        self.assertFalse((output / "source/plug.manifest").exists())

    def test_native_exact_name_and_profile_label_suffix_refusals(self):
        archive, _, manifest_hash = self.fixture()
        artifact, admission = self.metadata(archive, manifest_hash)
        self.assertEqual(artifact["name"], "plug-public-bundle-" + admission.label)
        with patch.object(helper, "NATIVE_ADMISSION", admission):
            helper.validate_metadata(self.run, artifact, "123", "456",
                                     profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
            for name in (artifact["name"] + "-123", artifact["name"] + "-999",
                         "plug-public-bundle-other-label",
                         "plug-public-bundle-verification-" + admission.label):
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "native metadata differs"):
                    helper.validate_metadata(self.run, {**artifact, "name": name}, "123", "456",
                                             profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
            for name in ("plug-public-importer-bundle-" + admission.label,
                         "plug-public-bundle-" + "x" * 49, "plug-public-bundle-" + admission.label + "/"):
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "immutable producer run"):
                    helper.validate_metadata(self.run, {**artifact, "name": name}, "123", "456",
                                             profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
            with self.assertRaisesRegex(ValueError, "immutable producer run"):
                helper.validate_metadata(self.run, artifact, "123", "456", profile=helper.PROFILE)
            with self.assertRaisesRegex(ValueError, "native metadata differs"):
                helper.validate_metadata(self.run, self.artifact, "123", "456",
                                         profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
            with self.assertRaisesRegex(ValueError, "unsupported public metadata profile"):
                helper.validate_metadata(self.run, artifact, "123", "456", profile="plug-public-native/2")
        with patch.object(helper, "NATIVE_ADMISSION", None), self.assertRaisesRegex(ValueError, "no trusted official-byte"):
            helper.validate_metadata(self.run, artifact, "123", "456",
                                     profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)

    def test_profile_relabel_or_unknown_profile_is_refused(self):
        for schema in (helper.PROFILE, "plug-public-bundle/1", "plug-public-native/2"):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                self.check_fixture(mutate_manifest=lambda m: m.update(schema=schema))

    def test_native_exact_source_build_toolchain_and_probe_fields(self):
        mutations = (
            lambda m: m.update(extra="not permitted"),
            lambda m: m["source"].update(plug={}),
            lambda m: m["source"]["beads"].update(commit="b" * 40),
            lambda m: m["source"]["beads"].update(tree="b" * 40),
            lambda m: m["source"]["beads"].update(manifest_sha256="b" * 64),
            lambda m: m["source"]["beads"].update(manifest_entries=1),
            lambda m: m["source"]["beads"].update(extra=True),
            lambda m: m["source_refs"].update(plug="not permitted"),
            lambda m: m["targets"].update(plug_roundtrip={}),
            lambda m: m["targets"]["beads_exporter"].update(label="//cmd/bd:bd_for_tests"),
            lambda m: m["build"].update(plug="not permitted"),
            lambda m: m["build"].update(beads="CGO_ENABLED=0 go build ./cmd/bd"),
            lambda m: m["build"].update(run_id="999"),
            lambda m: m["toolchain"].update(go="go version unreviewed"),
            lambda m: m["toolchain"].update(bazelisk_sha256="b" * 64),
            lambda m: m["resolution"].update(roundtrip_help_exit=2),
            lambda m: m["resolution"].pop("bd_init_help_sha256"),
            lambda m: m["resolution"].update(bd_export_help_exit=False),
            lambda m: m["resolution"].update(bd_export_help_exit=1),
            lambda m: m["resolution"].update(bd_import_help_sha256="invalid"),
            lambda m: m["resolution"]["bd_export_version"].update(commit="b" * 40),
            lambda m: m["workflow"].update(sha="b" * 40),
            lambda m: m["archive"].update(extra=True),
            lambda m: m["artifacts"]["bin/bd-export"].update(size=True),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.check_fixture(mutate_manifest=mutation)

    def test_native_requires_separately_admitted_official_metadata(self):
        archive, output, manifest_hash = self.fixture()
        artifact, admission = self.metadata(archive, manifest_hash)
        with patch.object(helper, "NATIVE_ADMISSION", None), self.assertRaisesRegex(ValueError, "no trusted official-byte"):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123", artifact=artifact)
        for field, value in (("descriptor", "123:789:" + manifest_hash), ("workflow_sha", "b" * 40),
                             ("workflow_branch", "wrong"), ("label", "wrong"),
                             ("artifact_digest", "sha256:" + "b" * 64), ("artifact_size", admission.artifact_size + 1)):
            with self.subTest(field=field), patch.object(helper, "NATIVE_ADMISSION", admission._replace(**{field: value})), self.assertRaises(ValueError):
                helper.validate_metadata(self.run, artifact, "123", "456", profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
        with patch.object(helper, "NATIVE_ADMISSION", admission):
            for field, value in (("workflow_id", 1), ("run_attempt", 2), ("actor", {"login": "other"}),
                                 ("triggering_actor", {"login": "other"}), ("repository", {"full_name": "someone/else"})):
                with self.subTest(field=field), self.assertRaises(ValueError):
                    helper.validate_metadata({**self.run, field: value}, artifact, "123", "456", profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)

    def test_native_container_corruption_and_missing_members(self):
        for name in ("bin/roundtrip", "source/plug.manifest", "../escape", "/absolute"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_zip=lambda files: files.update({name: b"extra"}))
        for name in ("manifest.json", "receipt.json", "source/beads.manifest", "bin/bd-export"):
            with self.subTest(missing=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_zip=lambda files: files.pop(name))
        for name in ("receipt.json", "source/beads.manifest", "bin/bd-export", "bundle.tar"):
            with self.subTest(corrupt=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_zip=lambda files: files.update({name: b"corrupt"}))
        for mutation in (lambda member: setattr(member, "mode", 0o644),
                         lambda member: setattr(member, "mode", 0o4755),
                         lambda member: setattr(member, "type", tarfile.SYMTYPE),
                         lambda member: setattr(member, "type", tarfile.LNKTYPE),
                         lambda member: setattr(member, "name", "../escape")):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.check_fixture(mutate_tar=mutation)
        self.assertFalse((self.root / "escape").exists())

    def test_native_zip_links_and_duplicates_are_refused_before_extraction(self):
        archive, output, manifest_hash = self.fixture()
        with zipfile.ZipFile(archive) as stream:
            files = {member.filename: stream.read(member) for member in stream.infolist()}
        with zipfile.ZipFile(archive, "w") as stream:
            for name, payload in files.items():
                member = zipfile.ZipInfo(name)
                member.external_attr = ((stat.S_IFLNK | 0o777) if name == "bin/bd-export" else (stat.S_IFREG | 0o644)) << 16
                stream.writestr(member, payload)
        with self.assertRaisesRegex(ValueError, "unsafe member"):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123")
        archive, output, manifest_hash = self.fixture()
        with zipfile.ZipFile(archive, "a") as stream:
            stream.writestr("bin/bd-export", b"duplicate")
        with self.assertRaisesRegex(ValueError, "file set"):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123")

    def test_native_tar_extra_or_duplicate_members_refuse_with_valid_outer_hashes(self):
        for extra_name in ("bin/roundtrip", "bin/bd-export"):
            archive, output, _ = self.fixture()
            with zipfile.ZipFile(archive) as stream:
                files = {member.filename: stream.read(member) for member in stream.infolist()}
            canonical = io.BytesIO()
            with tarfile.open(fileobj=canonical, mode="w") as stream:
                for name in ("bin/bd-export", extra_name):
                    payload = b"fixture tar bytes"
                    member = tarfile.TarInfo(name)
                    member.mode, member.size = 0o755, len(payload)
                    stream.addfile(member, io.BytesIO(payload))
            files["bundle.tar"] = canonical.getvalue()
            manifest = json.loads(files["manifest.json"])
            manifest["archive"] = {"name": "bundle.tar", "size": len(files["bundle.tar"]),
                                   "sha256": hashlib.sha256(files["bundle.tar"]).hexdigest()}
            files["manifest.json"] = files["receipt.json"] = json.dumps(manifest).encode()
            manifest_hash = hashlib.sha256(files["manifest.json"]).hexdigest()
            with zipfile.ZipFile(archive, "w") as stream:
                for name, payload in files.items():
                    stream.writestr(name, payload)
            artifact, admission = self.metadata(archive, manifest_hash)
            with self.subTest(extra_name=extra_name), patch.object(helper, "NATIVE_ADMISSION", admission), self.assertRaisesRegex(ValueError, "archive member set"):
                helper.prepare_archive(archive, output, manifest_hash, self.run, "123", artifact=artifact)

    def test_duplicate_json_keys_are_refused_with_matching_manifest_hash(self):
        archive, output, _ = self.fixture()
        with zipfile.ZipFile(archive) as stream:
            files = {member.filename: stream.read(member) for member in stream.infolist()}
        files["manifest.json"] = b'{"schema":"plug-public-native/1",' + files["manifest.json"][1:]
        files["receipt.json"] = files["manifest.json"]
        manifest_hash = hashlib.sha256(files["manifest.json"]).hexdigest()
        with zipfile.ZipFile(archive, "w") as stream:
            for name, payload in files.items():
                stream.writestr(name, payload)
        artifact, admission = self.metadata(archive, manifest_hash)
        with patch.object(helper, "NATIVE_ADMISSION", admission), self.assertRaisesRegex(ValueError, "duplicate"):
            helper.prepare_archive(archive, output, manifest_hash, self.run, "123", artifact=artifact)

    def test_size_and_metadata_bounds_are_independent_of_matching_checksums(self):
        archive, output, manifest_hash = self.fixture()
        artifact, admission = self.metadata(archive, manifest_hash)
        with patch.object(helper, "MAX_ARTIFACT_BYTES", artifact["size_in_bytes"] - 1), self.assertRaises(ValueError):
            helper.validate_metadata(self.run, artifact, "123", "456",
                                     profile=helper.NATIVE_PROFILE, manifest_sha256=manifest_hash)
        with self.assertRaisesRegex(ValueError, "metadata bound"):
            self.check_fixture(mutate_zip=lambda files: files.update({"manifest.json": b" " * (1024 * 1024 + 1)}))
        with zipfile.ZipFile(archive) as stream:
            expanded = sum(member.file_size for member in stream.infolist())
            self.assertLess(max(member.file_size for member in stream.infolist()), expanded - 1)
        with patch.object(helper, "MAX_ARTIFACT_BYTES", expanded - 1), self.assertRaisesRegex(ValueError, "expands beyond"):
            self.check_fixture()

    def test_prepare_publishes_exactly_four_values_only_after_checks_and_never_executes_payload(self):
        archive, output, manifest_hash = self.fixture()
        artifact, admission = self.metadata(archive, manifest_hash)
        environment, evidence = self.root / "github-env", self.root / "preparation.json"
        calls = []

        def download(argv, **kwargs):
            calls.append(argv)
            self.assertEqual(argv, ["gh", "api", "repos/" + helper.REPOSITORY + "/actions/artifacts/456/zip"])
            kwargs["stdout"].write(archive.read_bytes())
            return type("Result", (), {"returncode": 0})()

        with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "NATIVE_ADMISSION", admission), patch.object(helper, "gh_json", side_effect=[self.run, artifact]), patch.object(helper.subprocess, "run", side_effect=download):
            helper.prepare(admission.descriptor, output, environment, evidence)
        receipt = json.loads(evidence.read_text())
        published = dict(line.split("=", 1) for line in environment.read_text().splitlines())
        self.assertEqual(set(published), set(helper.PUBLIC_ENV_NAMES))
        self.assertEqual(receipt["environment"], published)
        self.assertEqual(receipt["descriptor"], admission.descriptor)
        self.assertEqual(receipt["schema"], helper.NATIVE_PROFILE)
        self.assertEqual(receipt["artifact_digest"], artifact["digest"])
        self.assertEqual(receipt["producer_workflow_sha"], self.run["head_sha"])
        self.assertEqual(len(calls), 1)

    def test_prepare_cannot_select_native_from_name_without_exact_admitted_descriptor(self):
        archive, output, manifest_hash = self.fixture()
        artifact, admission = self.metadata(archive, manifest_hash)
        unadmitted = "123:456:" + ("0" * 64 if manifest_hash != "0" * 64 else "1" * 64)
        environment, evidence = self.root / "unadmitted-env", self.root / "unadmitted-receipt"
        with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "NATIVE_ADMISSION", admission), patch.object(helper, "gh_json", side_effect=[self.run, artifact]), patch.object(helper.subprocess, "run") as download, self.assertRaises(ValueError):
            helper.prepare(unadmitted, output, environment, evidence)
        download.assert_not_called()
        self.assertFalse(output.exists())
        self.assertFalse(environment.exists())
        self.assertFalse(evidence.exists())

    def test_prepare_binds_downloaded_schema_to_trusted_selected_profile(self):
        archive, output, manifest_hash = self.fixture(mutate_manifest=lambda m: m.update(schema=helper.PROFILE))
        artifact, admission = self.metadata(archive, manifest_hash)
        environment, evidence = self.root / "profile-env", self.root / "profile-receipt"

        def download(argv, **kwargs):
            self.assertEqual(argv, ["gh", "api", "repos/" + helper.REPOSITORY + "/actions/artifacts/456/zip"])
            kwargs["stdout"].write(archive.read_bytes())
            return type("Result", (), {"returncode": 0})()

        with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "NATIVE_ADMISSION", admission), patch.object(helper, "gh_json", side_effect=[self.run, artifact]), patch.object(helper.subprocess, "run", side_effect=download) as fetch, self.assertRaisesRegex(ValueError, "profile differs from trusted preparation selection"):
            helper.prepare(admission.descriptor, output, environment, evidence)
        self.assertEqual(fetch.call_count, 1)
        self.assertFalse((output / "bin/bd-export").exists())
        self.assertFalse(environment.exists())
        self.assertFalse(evidence.exists())

    def test_download_or_admission_failure_publishes_neither_receipt_nor_env(self):
        for failure in ("download", "zip-digest", "admission"):
            archive, output, manifest_hash = self.fixture()
            artifact, admission = self.metadata(archive, manifest_hash)
            environment, evidence = self.root / (failure + "-env"), self.root / (failure + "-receipt")

            def download(argv, **kwargs):
                kwargs["stdout"].write(archive.read_bytes() if failure != "zip-digest" else b"corrupt")
                return type("Result", (), {"returncode": 1 if failure == "download" else 0})()

            with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "NATIVE_ADMISSION", None if failure == "admission" else admission), patch.object(helper, "gh_json", side_effect=[self.run, artifact]), patch.object(helper.subprocess, "run", side_effect=download), self.assertRaises(ValueError):
                helper.prepare(admission.descriptor, output, environment, evidence)
            self.assertFalse(environment.exists())
            self.assertFalse(evidence.exists())

    def test_ambient_values_or_existing_or_linked_output_refuse_before_api(self):
        environment, evidence = self.root / "env", self.root / "evidence"
        for name in (*helper.PUBLIC_ENV_NAMES, "PLUG_PUBLIC_BEADS_EXTRA"):
            with patch.dict(helper.os.environ, {name: "caller"}, clear=True), patch.object(helper, "gh_json") as api, self.assertRaises(ValueError):
                helper.prepare("123:456:" + "a" * 64, self.root / "prepared", environment, evidence)
            api.assert_not_called()
        owned = self.root / "owned"
        owned.mkdir()
        link = self.root / "link"
        link.symlink_to(owned, target_is_directory=True)
        for output in (owned, link / "prepared"):
            with patch.dict(helper.os.environ, {}, clear=True), patch.object(helper, "gh_json") as api, self.assertRaises(ValueError):
                helper.prepare("123:456:" + "a" * 64, output, environment, evidence)
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
