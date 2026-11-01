#!/usr/bin/env python3
"""Run a pinned source-built recorder when one is supplied; never invent output.

The wrapper intentionally has no default executable. A future RunsOn invocation
must provide the Beads source checkout and executable explicitly, verify both
the pinned commit and tree, run twice in independently created disposable
stores, and compare raw/projection/provenance bytes before publishing a vector.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from projection import project


PIN = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_source(root):
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    tree = subprocess.check_output(["git", "-C", str(root), "rev-parse", f"{commit}^{{tree}}"], text=True).strip()
    if commit != PIN or tree != TREE:
        raise RuntimeError(f"source pin mismatch: commit={commit} tree={tree}")


def run_once(card, executable, source_root):
    verify_source(source_root)
    work = Path(tempfile.mkdtemp(prefix=f"s1-{card.lower()}-", dir="/tmp/plug-swarm/work"))
    try:
        env = {
            "PATH": os.environ.get("PATH", ""),
            "TZ": "UTC",
            "LC_ALL": "C.UTF-8",
            "BEADS_ACTOR": "plug-s1-recorder",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
        }
        result = subprocess.run(
            [str(executable), "--plug-s1-card", card], cwd=work, env=env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"reference recorder failed ({result.returncode}): {result.stderr}")
        raw = json.loads(result.stdout)
        projection = project(raw)
        return {
            "card": card,
            "sourceCommit": PIN,
            "sourceTree": TREE,
            "rawJSON": json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            "rawDigest": digest(json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()),
            "projection": projection,
        }
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("card", choices=("B09", "B10", "B11", "B12"))
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.reference.is_file():
        raise SystemExit("BLOCKED: exact source-built reference executable is unavailable")
    first = run_once(args.card, args.reference, args.source_root)
    second = run_once(args.card, args.reference, args.source_root)
    if first != second:
        raise SystemExit("BLOCKED: independent source recordings differ")
    args.output.write_text(json.dumps({"first": first, "repeat": second}, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
