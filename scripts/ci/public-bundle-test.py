#!/usr/bin/env python3
"""Regression checks for the public executable bundle boundary."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import stat
import tempfile


def load_helper():
    path = Path(__file__).with_name("public-bundle.py")
    spec = importlib.util.spec_from_file_location("public_bundle", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load public bundle helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def expect_refusal(function, message: str) -> None:
    try:
        function()
    except (OSError, ValueError, json.JSONDecodeError):
        return
    raise AssertionError(message)


def main() -> None:
    helper = load_helper()
    root = Path(tempfile.mkdtemp(prefix="plug-public-bundle-test-"))
    try:
        for name in ("bd", "plug", "bd-export", "roundtrip"):
            source = root / name
            source.write_bytes(b"#!/bin/sh\nexit 0\n")
            source.chmod(0o755)
        (root / "beads.manifest").write_text("beads\n", encoding="utf-8")
        (root / "plug.manifest").write_text("plug\n", encoding="utf-8")
        (root / "metadata.json").write_text(
            json.dumps(
                {
                    "build": {},
                    "toolchain": {},
                    "resolution": {},
                    "workflow": {"ref": "refs/heads/test", "sha": "a" * 40, "profile": helper.SCHEMA},
                    "source_refs": {"beads": "refs/remotes/origin/beads", "plug": "test"},
                }
            ),
            encoding="utf-8",
        )
        output = root / "bundle"
        helper.create(
            type("Args", (), {
                "output": output,
                "bd": str(root / "bd"),
                "plug": str(root / "plug"),
                "profile": helper.SCHEMA,
                "metadata": str(root / "metadata.json"),
                "beads_commit": "b" * 40,
                "beads_tree": "c" * 40,
                "beads_manifest": str(root / "beads.manifest"),
                "plug_commit": "d" * 40,
                "plug_tree": "e" * 40,
                "plug_manifest": str(root / "plug.manifest"),
                "plug_target": "//tools/roundtrip:roundtrip",
            })(),
        )
        helper.verify(type("Args", (), {"directory": output})())
        old_manifest = helper.read_json(output / "manifest.json")
        assert old_manifest["schema"] == helper.SCHEMA
        assert set(old_manifest["artifacts"]) == {"bin/bd", "bin/plug"}
        assert old_manifest["workflow"]["sha"] == "a" * 40
        assert old_manifest["source_refs"]["beads"] == "refs/remotes/origin/beads"

        companion_output = root / "companion"
        companion_metadata = root / "companion-metadata.json"
        companion_metadata.write_text(
            json.dumps(
                {
                    "build": {},
                    "toolchain": {},
                    "resolution": {},
                    "workflow": {"ref": "refs/heads/test", "sha": "a" * 40, "profile": helper.COMPANION_SCHEMA},
                    "source_refs": {"beads": "refs/remotes/origin/beads", "plug": "test"},
                }
            ),
            encoding="utf-8",
        )
        helper.create(
            type("Args", (), {
                "output": companion_output,
                "bd_export": str(root / "bd-export"),
                "roundtrip": str(root / "roundtrip"),
                "profile": helper.COMPANION_SCHEMA,
                "metadata": str(companion_metadata),
                "beads_commit": "f" * 40,
                "beads_tree": "a" * 40,
                "beads_manifest": str(root / "beads.manifest"),
                "plug_commit": "1" * 40,
                "plug_tree": "2" * 40,
                "plug_manifest": str(root / "plug.manifest"),
                "plug_target": "//tools/roundtrip:roundtrip",
            })(),
        )
        helper.verify(type("Args", (), {"directory": companion_output})())
        companion_manifest = helper.read_json(companion_output / "manifest.json")
        assert companion_manifest["schema"] == helper.COMPANION_SCHEMA
        assert set(companion_manifest["artifacts"]) == {"bin/bd-export", "bin/roundtrip"}
        assert companion_manifest["targets"]["beads_exporter"]["label"] == "//cmd/bd:bd"
        assert companion_manifest["targets"]["plug_roundtrip"]["binary"] == "bin/roundtrip"
        receipt = helper.read_json(companion_output / "receipt.json")
        assert receipt["schema"] == helper.COMPANION_SCHEMA
        assert receipt["workflow"]["profile"] == helper.COMPANION_SCHEMA
        assert (companion_output / "source/beads.manifest").read_bytes() == (root / "beads.manifest").read_bytes()
        assert (companion_output / "source/plug.manifest").read_bytes() == (root / "plug.manifest").read_bytes()

        assert helper.NATIVE_BEADS_SOURCE_REF == (
            "refs/remotes/origin/swarm/native-producer-census-dedup-20261104"
        )
        assert helper.NATIVE_BEADS_MANIFEST_ENTRIES == 5141
        assert helper.NATIVE_BEADS_MANIFEST_SHA256 == "e01a584042b4a2c32ea3cbcbdd55069f7c23bc2b853f8734857c5082e828f15d"
        native_fixture = "".join(
            f"100644 blob {index:040x}\tpath/{index}\n"
            for index in range(helper.NATIVE_BEADS_MANIFEST_ENTRIES)
        )
        native_manifest_path = root / "native-beads.manifest"
        native_manifest_path.write_text(native_fixture, encoding="utf-8")
        helper.NATIVE_BEADS_MANIFEST_SHA256 = helper.digest(native_manifest_path)
        native_output = root / "native"
        native_metadata = root / "native-metadata.json"
        native_metadata.write_text(
            json.dumps(
                {
                    "workflow": {
                        "ref": "refs/heads/test",
                        "sha": "a" * 40,
                        "profile": helper.NATIVE_SCHEMA,
                    },
                    "source_refs": {"beads": helper.NATIVE_BEADS_SOURCE_REF},
                    "toolchain": {
                        "go": "go version go1.26.10 linux/amd64",
                        "bazel": "bazel 9.3.0",
                        "bazelisk_sha256": "b" * 64,
                    },
                    "resolution": {
                        "bd_export_help_sha256": "c" * 64,
                        "bd_export_help_exit": 0,
                        "bd_export_version": {
                            "commit": helper.NATIVE_BEADS_COMMIT,
                            "build": "public-" + helper.NATIVE_BEADS_COMMIT,
                        },
                        "bd_export_version_sha256": "d" * 64,
                        "bd_init_help_sha256": "e" * 64,
                        "bd_import_help_sha256": "f" * 64,
                    },
                    "build": {"beads": "bazel build //cmd/bd:bd", "run_id": "12345"},
                }
            ),
            encoding="utf-8",
        )
        native_args = type("Args", (), {
            "output": native_output,
            "bd_export": str(root / "bd-export"),
            "profile": helper.NATIVE_SCHEMA,
            "metadata": str(native_metadata),
            "beads_commit": helper.NATIVE_BEADS_COMMIT,
            "beads_tree": helper.NATIVE_BEADS_TREE,
            "beads_manifest": str(native_manifest_path),
        })()
        helper.create(native_args)
        helper.verify(type("Args", (), {"directory": native_output})())
        native_manifest = helper.read_json(native_output / "manifest.json")
        assert native_manifest["schema"] == helper.NATIVE_SCHEMA
        assert set(native_manifest["artifacts"]) == {"bin/bd-export"}
        assert set(native_manifest["source"]) == {"beads"}
        assert native_manifest["targets"] == {
            "beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"},
        }
        assert native_manifest["build"]["run_id"] == "12345"
        assert (native_output / "source/beads.manifest").read_bytes() == native_manifest_path.read_bytes()
        native_expected = {
            field: copy.deepcopy(native_manifest[field])
            for field in ("schema", "source", "targets", "workflow", "source_refs")
        }
        native_expected["source"] = {
            name: {field: record[field] for field in ("commit", "tree")}
            for name, record in native_expected["source"].items()
        }
        native_expected_path = root / "native-expected-provenance.json"
        helper.write_json(native_expected_path, native_expected)
        native_pinned = type("Args", (), {
            "directory": native_output,
            "expected_provenance": native_expected_path,
        })()
        helper.verify(native_pinned)

        # Keep the retrieval ref exact, including verification without an
        # expected-provenance sidecar. Equal trees do not permit another commit.
        old_source_ref = "refs/remotes/origin/codex/refill-beads-native-inventory-20261102-0610"
        altered = copy.deepcopy(native_manifest)
        altered["source_refs"]["beads"] = old_source_ref
        expect_refusal(
            lambda: helper.verify_native_manifest_contract(altered),
            "native verifier accepted the former moving source ref",
        )
        altered = copy.deepcopy(native_manifest)
        altered["source"]["beads"]["commit"] = "dcff9b28739f35dff7ec45660623509788b27cbb"
        expect_refusal(
            lambda: helper.verify_native_manifest_contract(altered),
            "native verifier accepted a different commit with the same tree",
        )
        refused_metadata = root / "native-old-ref-metadata.json"
        altered_metadata = helper.read_json(native_metadata)
        altered_metadata["source_refs"]["beads"] = old_source_ref
        helper.write_json(refused_metadata, altered_metadata)
        refused_args = copy.copy(native_args)
        refused_args.output = root / "native-old-ref-refused"
        refused_args.metadata = str(refused_metadata)
        expect_refusal(
            lambda: helper.create(refused_args),
            "native create accepted the former moving source ref",
        )

        native_with_plug_arg = type("Args", (), {
            "output": root / "native-refused",
            "bd_export": str(root / "bd-export"),
            "plug_target": "//cmd/plug:plug",
            "profile": helper.NATIVE_SCHEMA,
            "metadata": str(native_metadata),
            "beads_commit": helper.NATIVE_BEADS_COMMIT,
            "beads_tree": helper.NATIVE_BEADS_TREE,
            "beads_manifest": str(native_manifest_path),
        })()
        expect_refusal(lambda: helper.create(native_with_plug_arg), "native profile accepted a dummy Plug target")
        for field, value in (
            ("source", {"plug": {"commit": "1" * 40, "tree": "2" * 40}}),
            ("source_refs", {"plug": "wrong-source-ref"}),
            ("targets", {"plug_roundtrip": {"label": "//tools/roundtrip:roundtrip", "binary": "bin/roundtrip"}}),
            ("artifacts", {"bin/roundtrip": {"sha256": "0" * 64, "size": 1, "mode": "0o755"}}),
            ("build", {"run_id": "001"}),
        ):
            altered = copy.deepcopy(native_manifest)
            if field in {"source", "source_refs", "targets", "artifacts"}:
                altered[field].update(value)
            else:
                altered[field].update(value)
            helper.write_json(native_output / "manifest.json", altered)
            expect_refusal(lambda: helper.verify(native_pinned), f"native {field} mutation was accepted")
        helper.write_json(native_output / "manifest.json", native_manifest)
        helper.verify(native_pinned)

        native_source_manifest = native_output / "source/beads.manifest"
        native_source_manifest.write_bytes(b"wrong native source\n")
        expect_refusal(lambda: helper.verify(native_pinned), "native source manifest mutation was accepted")
        native_source_manifest.write_bytes(native_manifest_path.read_bytes())
        helper.verify(native_pinned)
        helper.NATIVE_BEADS_MANIFEST_SHA256 = "e01a584042b4a2c32ea3cbcbdd55069f7c23bc2b853f8734857c5082e828f15d"

        expected = {
            field: copy.deepcopy(companion_manifest[field])
            for field in ("schema", "source", "targets", "workflow", "source_refs")
        }
        expected["source"] = {
            name: {field: record[field] for field in ("commit", "tree")}
            for name, record in expected["source"].items()
        }
        expected_path = root / "expected-provenance.json"
        helper.write_json(expected_path, expected)
        pinned_check = type("Args", (), {"directory": companion_output, "expected_provenance": expected_path})()
        helper.verify(pinned_check)

        # A workflow commit is not an interchangeable roundtrip source. Keep
        # source SHA/tree, refs, target and workflow binding checks independent
        # of the payload/archive digest checks.
        for field, name, key, value in (
            ("source", "plug", "commit", "a" * 40),
            ("source", "beads", "tree", "b" * 40),
            ("workflow", None, "sha", "c" * 40),
            ("source_refs", None, "plug", "wrong-source-ref"),
            ("targets", "plug_roundtrip", "label", "//cmd/plug:plug"),
        ):
            altered = copy.deepcopy(companion_manifest)
            destination = altered[field] if name is None else altered[field][name]
            destination[key] = value
            helper.write_json(companion_output / "manifest.json", altered)
            expect_refusal(lambda: helper.verify(pinned_check), f"{field} binding mutation was accepted")
        helper.write_json(companion_output / "manifest.json", companion_manifest)
        helper.verify(pinned_check)

        source_manifest = companion_output / "source/beads.manifest"
        source_manifest.write_bytes(b"wrong canonical source\n")
        expect_refusal(lambda: helper.verify(pinned_check), "canonical source manifest mutation was accepted")
        source_manifest.write_bytes((root / "beads.manifest").read_bytes())
        helper.verify(pinned_check)

        loose = output / "bin" / "plug"
        original = loose.read_bytes()
        loose.write_bytes(original + b"tampered")
        expect_refusal(lambda: helper.verify(type("Args", (), {"directory": output})()), "loose content mutation was accepted")
        loose.write_bytes(original)
        loose.chmod(stat.S_IMODE(loose.stat().st_mode) ^ 0o001)
        expect_refusal(lambda: helper.verify(type("Args", (), {"directory": output})()), "loose mode mutation was accepted")

        companion_loose = companion_output / "bin" / "roundtrip"
        companion_original = companion_loose.read_bytes()
        companion_loose.write_bytes(companion_original + b"tampered")
        expect_refusal(
            lambda: helper.verify(type("Args", (), {"directory": companion_output})()),
            "companion loose content mutation was accepted",
        )
        companion_loose.write_bytes(companion_original)
        companion_loose.chmod(stat.S_IMODE(companion_loose.stat().st_mode) ^ 0o001)
        expect_refusal(
            lambda: helper.verify(type("Args", (), {"directory": companion_output})()),
            "companion loose mode mutation was accepted",
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
    print("public-bundle-regression: create/verify and loose mutation refusals passed")
