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

    def b06_vector(self, reports=None, human=None):
        """Authored transcript fixtures only; no native executable is run."""
        paths = [["b06-alpha", "b06-middle", "b06-zulu"], ["b06-beta", "b06-november", "b06-yankee"]]
        expanded = [{"partial": False, "members": [{"id": item, "issue": {"id": item, "title": item}} for item in path]} for path in paths]
        reports = copy.deepcopy(reports if reports is not None else [[], [], [], expanded, expanded, expanded, expanded])
        reports = iter(reports)

        def rendering(shown):
            text = "\nFound 2 dependency cycles:\n\n"
            for index, path in enumerate(paths[:shown], 1):
                text += f"{index}. Cycle involving:\n" + "".join(f"   - {item}: {item}\n" for item in path) + "\n"
            if shown == 1:
                text += "... and 1 more not shown. The count above is the whole report; use --limit 0 to print all of it.\n\n"
            return text.encode()

        renderings = {"1": rendering(1), "0": rendering(2), **(human or {})}

        def native_boundary(binary, root, commands, args, expected=None, env=None):
            stderr = b""
            code = 0
            if args[:2] == ["dep", "cycles"]:
                output = json.dumps(next(reports)).encode() if "--json" in args else renderings[args[-1]]
            elif args[0] == "show":
                identifier = args[1]
                parents = {"b06-child": "b06-ancestor", "b06-descendant": "b06-child"}
                dependencies = [] if identifier not in parents else [
                    {"id": parents[identifier], "dependency_type": "parent-child"}]
                output = json.dumps([{"id": identifier, "dependencies": dependencies}]).encode()
            elif args in (["dep", "add", "b06-source", "b06-source", "--type", "blocks"],
                          ["dep", "add", "b06-source", "b06-source", "--type", "tracks"]):
                output, stderr, code = b"", b"unit fixture: cannot depend on itself\n", 1
            elif args in (["dep", "add", "b06-ancestor", "b06-descendant", "--type", "blocks"],
                          ["dep", "add", "b06-descendant", "b06-ancestor", "--type", "blocks"]):
                output, stderr, code = b"", b"unit fixture: blocking ancestor/descendant refused\n", 1
            elif args == ["dep", "add", "b06-target", "b06-source", "--type", "blocks"]:
                output, stderr, code = b"", b"unit fixture: dependency cycle refused\n", 1
            else:
                output = b"unit fixture; never a reference observation\n"
            self.assertEqual(code, 0 if expected is None else expected)
            command = {"argv": list(args), "exitCode": code,
                       "stdoutBase64": base64.b64encode(output).decode(), "stdoutSha256": sha(output),
                       "stderrBase64": base64.b64encode(stderr).decode(), "stderrSha256": sha(stderr)}
            try:
                command["stdout"] = json.loads(output)
            except (ValueError, UnicodeDecodeError):
                pass
            commands.append(command)
            return output

        with mock.patch.dict(os.environ, {"PLUG_REFERENCE_SOURCE_DIR": str(self.root), "PLUG_REFERENCE_CLOCK_PROOF": "unit-only"}), \
                mock.patch.object(recorder, "run", side_effect=native_boundary):
            return recorder.record("B06", self.root / "not-a-native-executable", self.root, "b" * 64, "c" * 64)

    def test_b06_supported_route_requires_nonempty_tracks_and_preserves_dependencies(self):
        vector = self.b06_vector()
        independent.verify_b06(vector)
        coverage = vector["observations"]["cycleProbeCoverage"]
        self.assertEqual(coverage["status"], "partial")
        self.assertEqual(coverage["blocked"], ["blocking-only-cycle-public-fixture", "parallel-rows-public-fixture", "unreadable-cycle-public-fixture"])
        for command in vector["commands"]:
            self.assertIn(command["argv"][0], ["init", "version", "create", "dep", "show"])
            self.assertFalse(set(command["argv"]) & {"import", "sql", "--no-cycle-check", "--force", "--allow-stale"})
        self.assertEqual(len(vector["ids"]), 13)
        self.assertEqual(len(vector["observations"]["cycleProbes"]), 9)
        self.assertEqual([probe["case"] for probe in vector["observations"]["guardRefusals"]],
                         ["self-blocks", "self-tracks", "ancestor-blocker", "descendant-blocker"])

    def test_b06_rejects_empty_unrotated_duplicate_partial_or_incomplete_reports(self):
        good = self.b06_vector()
        outputs = [command["stdout"] for command in good["commands"] if command["argv"][:2] == ["dep", "cycles"] and "--json" in command["argv"]]
        for variant in ("empty", "rotation", "duplicate", "partial", "missing-member", "pure-tracks", "default-tracks", "json-cap"):
            with self.subTest(variant=variant):
                changed = copy.deepcopy(outputs)
                if variant == "empty":
                    changed[3] = []
                elif variant == "rotation":
                    members = changed[3][0]["members"]
                    changed[3][0]["members"] = members[1:] + members[:1]
                elif variant == "duplicate":
                    changed[3].append(copy.deepcopy(changed[3][0]))
                elif variant == "partial":
                    changed[3][0]["partial"] = True
                    changed[3][0]["members"][1].pop("issue")
                elif variant == "missing-member":
                    changed[3][0]["members"].pop()
                elif variant == "pure-tracks":
                    changed[1] = changed[3]
                elif variant == "default-tracks":
                    changed[2] = changed[3]
                else:
                    changed[6] = changed[6][:1]
                with self.assertRaises(RuntimeError):
                    self.b06_vector(reports=changed)

    def test_b06_human_limit_keeps_total_and_json_stays_uncapped(self):
        for output in (b"Found 1 dependency cycles:\n", b"Found 2 dependency cycles:\n", b"not a cycle report"):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                self.b06_vector(human={"1": output})

    def test_independent_b06_rejects_rehashed_missing_or_forged_coverage(self):
        def stream(command, name, raw):
            command[name + "Base64"] = base64.b64encode(raw).decode()
            command[name + "Sha256"] = sha(raw)
            if name == "stdout" and "stdout" in command:
                command["stdout"] = json.loads(raw)

        for variant in ("no-probes", "complete-claim", "lost-dependency", "bypass", "lost-create", "wrong-edge", "failed-edge", "wrong-command", "empty-report", "forged-path",
                        "no-guards", "self-diagnostic", "hierarchy-diagnostic", "refused-mutation", "wrong-parent", "successful-refusal", "guard-index"):
            with self.subTest(variant=variant):
                vector = self.b06_vector()
                observations = vector["observations"]
                if variant == "no-probes":
                    observations["cycleProbes"] = []
                elif variant == "complete-claim":
                    observations["cycleProbeCoverage"]["status"] = "complete"
                elif variant == "lost-dependency":
                    observations["cycleProbeCoverage"]["blocked"] = []
                elif variant == "bypass":
                    vector["commands"][observations["cycleProbes"][0]["commandIndex"] - 2]["argv"].append("--no-cycle-check")
                elif variant == "lost-create":
                    index = next(i for i, command in enumerate(vector["commands"])
                                 if command["argv"][:3] == ["create", "--id", "b06-pure-a"])
                    vector["commands"].pop(index)
                elif variant == "wrong-edge":
                    command = next(command for command in vector["commands"]
                                   if command["argv"] == ["dep", "add", "b06-pure-a", "b06-pure-z", "--type", "tracks"])
                    command["argv"][-1] = "blocks"
                elif variant == "failed-edge":
                    command = next(command for command in vector["commands"]
                                   if command["argv"] == ["dep", "add", "b06-pure-a", "b06-pure-z", "--type", "tracks"])
                    command["exitCode"] = 1
                elif variant == "wrong-command":
                    index = observations["cycleProbes"][3]["commandIndex"]
                    vector["commands"][index]["argv"] = ["dep", "cycles", "--json"]
                elif variant == "empty-report":
                    probe = observations["cycleProbes"][3]
                    command = vector["commands"][probe["commandIndex"]]
                    command.update(stdout=[], stdoutBase64="W10=", stdoutSha256=sha(b"[]"))
                    probe["paths"] = []
                elif variant == "forged-path":
                    observations["cycleProbes"][3]["paths"] = [["forged-member"]]
                elif variant == "no-guards":
                    observations["guardRefusals"] = []
                elif variant in ("self-diagnostic", "hierarchy-diagnostic"):
                    probe = observations["guardRefusals"][0 if variant == "self-diagnostic" else 2]
                    stream(vector["commands"][probe["commandIndex"]], "stderr", b"unknown flag: self ancestor\n")
                elif variant == "refused-mutation":
                    probe = observations["guardRefusals"][2]
                    index = probe["afterIndices"][0]
                    changed = copy.deepcopy(vector["commands"][index]["stdout"])
                    changed[0]["title"] = "changed by refused edge"
                    stream(vector["commands"][index], "stdout", json.dumps(changed).encode())
                elif variant == "wrong-parent":
                    probe = observations["guardRefusals"][2]
                    for index in (probe["beforeIndices"][1], probe["afterIndices"][1]):
                        changed = copy.deepcopy(vector["commands"][index]["stdout"])
                        changed[0]["dependencies"][0]["dependency_type"] = "relates-to"
                        stream(vector["commands"][index], "stdout", json.dumps(changed).encode())
                elif variant == "successful-refusal":
                    index = observations["guardRefusals"][0]["commandIndex"]
                    vector["commands"][index]["exitCode"] = 0
                else:
                    observations["guardRefusals"][0]["afterIndices"] = observations["guardRefusals"][0]["beforeIndices"]
                # Valid fresh hashes must not turn missing semantics into a
                # successful independent projection. These bytes are unit
                # fixtures and are never written into the reference manifest.
                vectors = [{"card": card, "ids": ["unit-a"], "configuration": {"prefix": "unit"},
                            "commands": [], "observations": {"probeResults": [{"stdoutBase64": ""}]}}
                           for card in ("B05", "B06", "B07", "B08")]
                vectors[1] = vector
                value = {"status": "recorded", "recordings": [
                    {"run": run, "raw": copy.deepcopy(vectors), "rawSha256": sha(recorder.canonical(vectors).encode()),
                     "projectionSha256": sha(recorder.canonical(recorder.project(vectors)).encode())} for run in ("a", "b")
                ]}
                with self.assertRaises(RuntimeError):
                    independent.verify_recordings(value)

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
        vectors[1] = self.b06_vector()
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
