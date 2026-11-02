#!/usr/bin/env python3
"""Public fixture guard tests, not actual S1 capture or runtime evidence."""

from __future__ import annotations

import copy
import base64
from contextlib import ExitStack
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile


spec = importlib.util.spec_from_file_location("reference", Path(__file__).with_name("prepare-public-reference.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


class ReferenceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="public-reference-guard-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.index = 0
        self.run = {
            "id": int(helper.RUN), "status": "completed", "conclusion": "success",
            "event": "workflow_dispatch", "path": helper.bundle.WORKFLOW,
            "head_sha": helper.WORKFLOW_SHA, "head_branch": helper.WORKFLOW_BRANCH,
            "repository": {"full_name": helper.bundle.REPOSITORY},
            "head_repository": {"full_name": helper.bundle.REPOSITORY},
        }
        self.artifact = {
            "id": int(helper.ARTIFACT), "expired": False,
            "workflow_run": {"id": int(helper.RUN), "head_sha": helper.WORKFLOW_SHA},
            "name": "plug-public-bundle-reference-" + helper.RUN,
            "digest": "sha256:" + helper.ZIP_SHA256, "size_in_bytes": helper.ZIP_SIZE,
        }

    def test_marker_is_fixed_unique_disjoint_and_opt_in(self):
        marker = helper.MARKER + helper.DESCRIPTOR
        self.assertEqual(helper.descriptor_from_args("test //..."), "")
        self.assertEqual(helper.descriptor_from_args("test //... " + marker), helper.DESCRIPTOR)
        self.assertEqual(helper.descriptor_from_args("coverage //... " + marker), helper.DESCRIPTOR)
        for args in (
            "test //... " + marker + " " + marker,
            "test //... " + marker + " --test_env=PLUG_PUBLIC_BUNDLE=123:456:" + "a" * 64,
            "test //... --test_env=PLUG_PUBLIC_REFERENCE=" + helper.DESCRIPTOR.upper(),
            "test //... --test_env=PLUG_PUBLIC_REFERENCE=1:2:" + "a" * 64,
            "test //... --test_env=PLUG_PUBLIC_REFERENCE " + helper.DESCRIPTOR,
            "test //... " + marker + " --test_env=PLUG_PUBLIC_REFERENCE_SOURCE=/unverified",
            "test //... --test_env=PLUG_PUBLIC_REFERENCE_ZIP=/unverified",
            "build //... " + marker,
            "test //... " + marker + ";echo",
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                helper.descriptor_from_args(args)

    def test_immutable_success_repository_workflow_and_independent_zip_binding(self):
        helper.validate_metadata(self.run, self.artifact)
        for field, value in (("conclusion", "failure"), ("status", "in_progress"),
                             ("head_sha", "b" * 40), ("head_branch", "other"),
                             ("path", ".github/workflows/other.yml")):
            run = copy.deepcopy(self.run)
            run[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                helper.validate_metadata(run, self.artifact)
        for field, value in (("id", 1), ("digest", "sha256:" + "a" * 64),
                             ("expired", True), ("size_in_bytes", 100)):
            artifact = copy.deepcopy(self.artifact)
            artifact[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                helper.validate_metadata(self.run, artifact)

    def prepared_environment(self):
        return {
            "PLUG_PUBLIC_REFERENCE_ZIP": str(self.root / "prepared public" / "reference.zip"),
            "PLUG_PUBLIC_REFERENCE_SOURCE": str(self.root / "prepared public" / "beads-source"),
            "PLUG_PUBLIC_REFERENCE_MANIFEST": str(self.root / "prepared public" / "manifest.json"),
            "PLUG_PUBLIC_REFERENCE_BUILD_RUN": helper.RUN,
        }

    def test_capture_routes_literal_prepared_paths_after_program_separator(self):
        environment = self.prepared_environment()
        marker = helper.MARKER + helper.DESCRIPTOR
        for separator in ("", " --"):
            arguments = "run " + helper.CAPTURE_TARGET + " --jobs=2 " + marker + separator
            self.assertEqual(helper.descriptor_from_args(arguments), helper.DESCRIPTOR)
            actual = helper.route_arguments(arguments, environment)
            self.assertEqual(actual, [
                "run", helper.CAPTURE_TARGET, "--jobs=2", "--",
                "--prepared-artifact", environment["PLUG_PUBLIC_REFERENCE_ZIP"],
                "--prepared-source", environment["PLUG_PUBLIC_REFERENCE_SOURCE"],
            ])
            self.assertFalse(any(arg.startswith("--test_env") for arg in actual))

    def test_capture_refuses_other_targets_program_args_or_ambiguous_routing(self):
        marker = helper.MARKER + helper.DESCRIPTOR
        prefix = "run " + helper.CAPTURE_TARGET + " " + marker
        for arguments in (
            "run //other:capture " + marker,
            "run " + helper.CAPTURE_TARGET + " //other:capture " + marker,
            "run " + helper.CAPTURE_TARGET + " -- " + marker,
            prefix + " -- --",
            prefix + " -- --prepared-artifact /unverified",
            prefix + " -- --prepared-source=/unverified",
            prefix + " --prepared-artifact=/unverified",
            prefix + " --prepared-source /unverified",
            prefix + " --test_arg=--prepared-source=/unverified",
            prefix + " --test_env=PLUG_PUBLIC_REFERENCE_ZIP=/unverified",
            prefix + " --test_env=OTHER=value",
            prefix + " -- --output=/unapproved",
            prefix + " --jobs 2 --",
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                helper.route_arguments(arguments, self.prepared_environment())

    def test_prepared_route_preserves_default_tests_and_only_explicit_public_env(self):
        self.assertEqual(helper.route_arguments("run //other:binary -- --help", {}),
                         ["run", "//other:binary", "--", "--help"])
        self.assertEqual(helper.route_arguments("test //...", {}), ["test", "//..."])
        environment = self.prepared_environment()
        environment["GH_TOKEN"] = "fixture-token-not-a-secret"
        marker = helper.MARKER + helper.DESCRIPTOR
        for command in ("test", "coverage"):
            self.assertEqual(helper.route_arguments(command + " //... " + marker, environment),
                             [command, "//...", marker] + [
                                 "--test_env=" + name + "=" + environment[name]
                                 for name in helper.ENVIRONMENT_NAMES
                             ])

    def test_prepared_route_requires_all_absolute_paths_and_exact_build_run(self):
        arguments = "run " + helper.CAPTURE_TARGET + " " + helper.MARKER + helper.DESCRIPTOR
        for name in helper.ENVIRONMENT_NAMES:
            for value in ("", "unexpected\nvalue", "unexpected\0value", "relative/path"):
                environment = self.prepared_environment()
                environment[name] = value
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    helper.route_arguments(arguments, environment)

    def test_route_cli_outputs_only_nul_delimited_literal_arguments(self):
        environment = {**helper.os.environ, **self.prepared_environment(),
                       "GH_TOKEN": "fixture-token-not-a-secret"}
        arguments = "run " + helper.CAPTURE_TARGET + " " + helper.MARKER + helper.DESCRIPTOR + " --"
        result = subprocess.run(
            [sys.executable, str(Path(helper.__file__)), "route", "--args", arguments],
            env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
        )
        expected = helper.route_arguments(arguments, environment)
        self.assertEqual(result.stdout, b"".join(helper.os.fsencode(arg) + b"\0" for arg in expected))
        self.assertEqual(result.stderr, b"")
        self.assertNotIn(b"fixture-token-not-a-secret", result.stdout)

    def fixture(self, mutate_manifest=None, mutate_tar=None, mutate_files=None):
        self.index += 1
        payloads = {"bin/bd": b"public fixture reference\n", "bin/plug": b"public fixture runtime\n"}
        binaries = {name: {"mode": "0o755", "size": len(value), "sha256": hashlib.sha256(value).hexdigest()}
                    for name, value in payloads.items()}
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            for name, payload in payloads.items():
                member = tarfile.TarInfo(name)
                member.mode, member.size = 0o755, len(payload)
                if mutate_tar:
                    mutate_tar(member)
                if not member.isreg():
                    member.size = 0
                archive.addfile(member, io.BytesIO(payload) if member.isreg() else None)
        canonical = buffer.getvalue()
        archive_record = {"name": "bundle.tar", "size": len(canonical), "sha256": hashlib.sha256(canonical).hexdigest()}
        manifest = {
            "schema": helper.PROFILE, "source": copy.deepcopy(helper.SOURCES),
            "targets": copy.deepcopy(helper.TARGETS), "artifacts": binaries, "archive": archive_record,
            "source_refs": {"beads": "refs/remotes/origin/beads", "plug": "codex/b1c-runtime-source-cb74bf48-20261102"},
            "workflow": {"ref": "refs/heads/" + helper.WORKFLOW_BRANCH, "sha": helper.WORKFLOW_SHA, "profile": helper.PROFILE},
            "build": {"run_id": helper.RUN, "beads": "CGO_ENABLED=0 go build -tags gms_pure_go -trimpath ./cmd/bd",
                      "plug": "bazel build --jobs=2 --local_resources=memory=8192 //cmd/plug:plug"},
            "resolution": {"beads_version_sha256": "affb43505816fa697a866678b7f0b22024508c6f78cb97ce2d4b6c6369fcacb6",
                           "plug_help_exit": 0, "plug_help_sha256": "6168f272cf1bc8e6a9437ff8859eaf54294153b7976f15c9795af9dd48c9d0f5"},
            "toolchain": {"bazel": "bazel 9.3.0", "go": "go version go1.26.10 linux/amd64",
                          "bazelisk_sha256": "5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992"},
        }
        if mutate_manifest:
            mutate_manifest(manifest)
        contents = json.dumps(manifest).encode()
        files = {**payloads, "bundle.tar": canonical, "manifest.json": contents, "receipt.json": contents}
        if mutate_files:
            mutate_files(files)
        path = self.root / f"fixture-{self.index}.zip"
        with zipfile.ZipFile(path, "w") as archive:
            for name, value in files.items():
                archive.writestr(name, value)
        constants = {"BINARIES": binaries, "ARCHIVE": archive_record,
                     "MANIFEST_SHA256": hashlib.sha256(contents).hexdigest(),
                     "ZIP_SHA256": hashlib.sha256(path.read_bytes()).hexdigest(), "ZIP_SIZE": path.stat().st_size}
        return path, constants

    def check_fixture(self, **kwargs):
        path, constants = self.fixture(**kwargs)
        with patch.multiple(helper, **constants):
            return helper.validate_zip(path, self.root / f"manifest-{self.index}.json")

    def test_valid_public_fixture_retains_raw_zip_without_executable_extraction(self):
        manifest = self.check_fixture()
        self.assertEqual(manifest["schema"], "plug-public-bundle/1")
        self.assertFalse((self.root / "bin").exists())
        self.assertEqual(len(list(self.root.glob("*.zip"))), 1)

    def test_cross_profile_and_source_tree_target_workflow_build_tampering(self):
        for mutate in (
            lambda m: m.update(schema="plug-public-companion/1"),
            lambda m: m["source"]["beads"].update(commit=helper.bundle.BEADS_COMMIT),
            lambda m: m["source"]["beads"].update(tree="b" * 40),
            lambda m: m["source"]["plug"].update(commit="b" * 40),
            lambda m: m["targets"]["beads_importer"].update(label="//cmd/bd:bd"),
            lambda m: m["workflow"].update(sha="b" * 40),
            lambda m: m["build"].update(beads="canonical Bazel"),
            lambda m: m["build"].update(run_id="123"),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.check_fixture(mutate_manifest=mutate)

    def test_zip_manifest_receipt_binary_and_archive_bytes_are_independent(self):
        path, constants = self.fixture()
        with patch.multiple(helper, **{**constants, "ZIP_SHA256": "0" * 64}), self.assertRaises(ValueError):
            helper.validate_zip(path, self.root / "no-manifest.json")
        with patch.multiple(helper, **{**constants, "MANIFEST_SHA256": "0" * 64}), self.assertRaises(ValueError):
            helper.validate_zip(path, self.root / "no-manifest.json")
        for name in ("receipt.json", "bin/bd", "bundle.tar"):
            value = b"{}" if name == "receipt.json" else b"tampered payload"
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_files=lambda files: files.update({name: value}))

    def test_links_traversal_duplicates_and_modes_refuse_before_publication(self):
        for mutate in (
            lambda member: setattr(member, "mode", 0o644),
            lambda member: setattr(member, "mode", 0o4755),
            lambda member: setattr(member, "type", tarfile.SYMTYPE),
            lambda member: setattr(member, "type", tarfile.LNKTYPE),
            lambda member: setattr(member, "name", "../escape"),
            lambda member: setattr(member, "name", "bin/bd"),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.check_fixture(mutate_tar=mutate)
        for name in ("../escape", "private-ledger.json", "/absolute"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check_fixture(mutate_files=lambda files: files.update({name: b"unexpected"}))
        self.assertFalse((self.root / "escape").exists())

    def test_zip_refuses_duplicate_and_symlink_members_even_with_matching_zip_hash(self):
        for symlink in (False, True):
            path, constants = self.fixture()
            if symlink:
                with zipfile.ZipFile(path) as archive:
                    files = {member.filename: archive.read(member) for member in archive.infolist()}
                with zipfile.ZipFile(path, "w") as archive:
                    for name, payload in files.items():
                        member = zipfile.ZipInfo(name)
                        member.external_attr = ((stat.S_IFLNK if name == "bin/bd" else stat.S_IFREG) | 0o755) << 16
                        archive.writestr(member, payload)
            else:
                with zipfile.ZipFile(path, "a") as archive:
                    archive.writestr("manifest.json", b"duplicate")
            constants.update(ZIP_SHA256=hashlib.sha256(path.read_bytes()).hexdigest(), ZIP_SIZE=path.stat().st_size)
            with self.subTest(symlink=symlink), patch.multiple(helper, **constants), self.assertRaises(ValueError):
                helper.validate_zip(path, self.root / "not-published.json")

    def test_download_failure_never_publishes_environment_or_fetches_source(self):
        environment = self.root / "github-env"
        environment.write_text("EXISTING=value\n")
        evidence = self.root / "evidence.json"
        def download(arguments, **kwargs):
            self.assertEqual(arguments, ["gh", "api", f"repos/{helper.bundle.REPOSITORY}/actions/artifacts/{helper.ARTIFACT}/zip"])
            kwargs["stdout"].write(b"invalid public fixture ZIP")
            return subprocess.CompletedProcess(arguments, 0)
        with patch.object(helper.bundle, "gh_json", side_effect=[self.run, self.artifact]), patch.object(helper.subprocess, "run", side_effect=download), patch.object(helper, "fetch_source") as fetch, self.assertRaises(ValueError):
            helper.prepare(helper.DESCRIPTOR, self.root / "prepared", environment, evidence)
        fetch.assert_not_called()
        self.assertEqual(environment.read_text(), "EXISTING=value\n")
        self.assertFalse(evidence.exists())

    def source_fixture(self):
        source = self.root / "source"
        source.mkdir()
        env = {**helper.git_environment(), "GIT_AUTHOR_NAME": "Public fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
               "GIT_COMMITTER_NAME": "Public fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid"}
        helper.git(source, ["init", "--quiet", "--template="], env)
        (source / "source.txt").write_text("public fixture source\n")
        (source / ".gitignore").write_text("ignored.txt\n")
        helper.git(source, ["add", "."], env)
        helper.git(source, ["commit", "--quiet", "-m", "public fixture"], env)
        manifest = helper.git(source, ["ls-tree", "-r", "--full-tree", "HEAD"])
        constants = {"SOURCE_SHA": helper.git(source, ["rev-parse", "HEAD"]).decode().strip(),
                     "SOURCE_TREE": helper.git(source, ["rev-parse", "HEAD^{tree}"]).decode().strip(),
                     "SOURCE_MANIFEST_SHA256": hashlib.sha256(manifest).hexdigest(), "SOURCE_ENTRIES": 2}
        return source, constants

    def test_source_requires_actual_clean_git_identity_and_canonical_manifest(self):
        source, constants = self.source_fixture()
        with patch.multiple(helper, **constants):
            self.assertEqual(helper.validate_source(source)["commit"], constants["SOURCE_SHA"])
            for name in ("SOURCE_SHA", "SOURCE_TREE", "SOURCE_MANIFEST_SHA256"):
                with self.subTest(name=name), patch.object(helper, name, "0" * len(constants[name])), self.assertRaises(ValueError):
                    helper.validate_source(source)
            for name in ("extra.txt", "ignored.txt"):
                (source / name).write_text("extra source\n")
                with self.subTest(name=name), self.assertRaises(ValueError):
                    helper.validate_source(source)
                (source / name).unlink()
            (source / "source.txt").write_text("dirty source\n")
            with self.assertRaises(ValueError):
                helper.validate_source(source)

    def test_source_refuses_persisted_remote_auth_and_hidden_index_entries(self):
        source, constants = self.source_fixture()
        with patch.multiple(helper, **constants):
            for key, value in (("remote.origin.url", helper.SOURCE_URL), ("http.extraheader", "fixture header")):
                helper.git(source, ["config", "--local", key, value])
                with self.subTest(key=key), self.assertRaises(ValueError):
                    helper.validate_source(source)
                helper.git(source, ["config", "--local", "--unset", key])
            helper.git(source, ["update-index", "--assume-unchanged", "source.txt"])
            with self.assertRaises(ValueError):
                helper.validate_source(source)

    def test_retrieval_token_is_scoped_to_fixed_fetch_environment_only(self):
        calls = []
        def record(source, arguments, environment=None):
            calls.append((arguments, helper.git_environment() if environment is None else environment.copy()))
            if arguments[0] == "fetch" and len([args for args, _ in calls if args[0] == "fetch"]) == 1:
                raise ValueError("fixture authenticated fetch refusal")
            return b""
        with patch.dict(helper.os.environ, {"GH_TOKEN": "fixture-token-not-a-secret"}), patch.object(helper, "git", side_effect=record), patch.object(helper, "validate_source", return_value=helper.SOURCES["beads"]):
            helper.fetch_source(self.root / "retrieved-source")
        fetches = [(args, env) for args, env in calls if args[0] == "fetch"]
        self.assertEqual(len(fetches), 2)
        args, env = fetches[0]
        fallback_args, fallback_env = fetches[1]
        self.assertIn("--no-write-fetch-head", args)
        self.assertIn("--depth=1", args)
        self.assertIn("--no-tags", args)
        self.assertEqual(args[-2:], [helper.SOURCE_URL, helper.SOURCE_SHA])
        self.assertEqual(fallback_args, args)
        self.assertEqual(fallback_args[-2:], [helper.SOURCE_URL, helper.SOURCE_SHA])
        self.assertEqual(
            base64.b64decode(env["GIT_CONFIG_VALUE_0"].split("AUTHORIZATION: Basic ", 1)[1]).decode(),
            "x-access-token:fixture-token-not-a-secret",
        )
        self.assertNotIn("fixture-token-not-a-secret", " ".join(fallback_env.values()))
        self.assertEqual(fallback_env, helper.git_environment())
        for arguments, environment in calls:
            self.assertNotIn("GH_TOKEN", environment)
            if arguments[0] != "fetch":
                self.assertFalse(any("fixture-token-not-a-secret" in value for value in environment.values()))


if __name__ == "__main__":
    unittest.main()
