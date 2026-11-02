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
        with patch.object(helper, "gh_json", side_effect=[{**self.run, "conclusion": "failure"}, self.artifact]):
            with self.assertRaises(ValueError):
                helper.prepare("123:456:" + "a" * 64, self.root / "prepared", environment, self.root / "evidence")
        self.assertFalse(environment.exists())
        self.assertFalse((self.root / "prepared").exists())


if __name__ == "__main__":
    unittest.main()
