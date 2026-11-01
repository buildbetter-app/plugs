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
        for name in ("bd", "plug"):
            source = root / name
            source.write_bytes(b"#!/bin/sh\nexit 0\n")
            source.chmod(0o755)
        (root / "beads.manifest").write_text("beads\n", encoding="utf-8")
        (root / "plug.manifest").write_text("plug\n", encoding="utf-8")
        (root / "metadata.json").write_text(
            json.dumps({"build": {}, "toolchain": {}, "resolution": {}}),
            encoding="utf-8",
        )
        output = root / "bundle"
        helper.create(
            type("Args", (), {
                "output": output,
                "bd": str(root / "bd"),
                "plug": str(root / "plug"),
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

        loose = output / "bin" / "plug"
        original = loose.read_bytes()
        loose.write_bytes(original + b"tampered")
        expect_refusal(lambda: helper.verify(type("Args", (), {"directory": output})()), "loose content mutation was accepted")
        loose.write_bytes(original)
        loose.chmod(stat.S_IMODE(loose.stat().st_mode) ^ 0o001)
        expect_refusal(lambda: helper.verify(type("Args", (), {"directory": output})()), "loose mode mutation was accepted")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
    print("public-bundle-regression: create/verify and loose mutation refusals passed")
