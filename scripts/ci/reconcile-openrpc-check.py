#!/usr/bin/env python3
"""Check the committed OpenRPC inputs without allowing reconciliation writes."""

import json
import os
from pathlib import Path
import subprocess
import sys


INPUTS = (
    "scripts/ci/reconcile-openrpc.py",
    "engdocs/design/plug/contracts/openrpc.json",
    "internal/api/wire/catalog.json",
    "internal/api/wire/wire_test.go",
)


def main():
    root = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
    paths = {relative: root / relative for relative in INPUTS}
    before = {relative: path.read_bytes() for relative, path in paths.items()}
    result = subprocess.run(
        [sys.executable, str(paths["scripts/ci/reconcile-openrpc.py"]), "--root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    changed = [relative for relative, path in paths.items() if path.read_bytes() != before[relative]]
    if changed:
        raise SystemExit("no-write reconciliation mutated inputs: " + ", ".join(changed))
    if result.returncode != 0:
        raise SystemExit(f"canonical reconciliation failed (exit {result.returncode}):\n{result.stderr}{result.stdout}")
    try:
        outcome = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"canonical reconciliation returned invalid JSON: {exc}") from exc
    if outcome.get("updated") != []:
        raise SystemExit(f"canonical reconciliation reported changes: {outcome.get('updated')!r}")
    print(result.stdout.strip())


if __name__ == "__main__":
    main()
