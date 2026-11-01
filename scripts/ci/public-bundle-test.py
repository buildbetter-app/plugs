#!/usr/bin/env python3
"""Regression checks for the public executable bundle boundary."""

from __future__ import annotations

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
