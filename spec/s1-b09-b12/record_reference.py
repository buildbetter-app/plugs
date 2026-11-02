#!/usr/bin/env python3
"""Record public B09-B12 behavior from the pinned Beads executable.

Each invocation creates a new Git-backed workspace. The command transcript is
kept separate from the card projection: timestamps, Dolt revisions, generated
owners and B09's timestamp-derived IDs are explicitly normalized, while the
stable behavior selected by each card is projected from the command results.
The wrapper refuses a source or binary pin mismatch and runs the card twice in
independent workspaces before writing an artifact.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


PIN = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
BINARY_SHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
DRIVER = "github.com/dolthub/dolt/go@v0.40.5-0.20260806213044-796d07497741"
WORK_ROOT = Path("/tmp/plug-swarm/work")

# These are runtime identities, not card semantics. They are removed from the
# retained transcript and named again in the limits field below.
NORMALIZED_FIELDS = {
    "created_at", "updated_at", "closed_at", "defer_until", "heartbeat_at",
    "started_at", "revision", "schema_version", "owner", "lease_expires_at",
    "leaseExpiresAt", "createdAt", "updatedAt", "closedAt", "commit", "hash",
    "CommitHash", "CommitDate", "revision_hash", "revisionHash",
}
RUNTIME_STAMP = re.compile(r"20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z")
WORKSPACE_STAMP = re.compile(r"/tmp/plug-swarm/work/[^/\s]+")

SOURCE_TESTS: dict[str, list[str]] = {
    "B09": [
        "internal/idgen/hash_test.go::TestGenerateHashIDMatchesJiraVector",
        "internal/storage/dolt/queries_test.go::TestCreateIssue_HashModeDefault",
    ],
    "B10": [
        "cmd/bd/protocol/child_id_test.go::TestProtocol_ChildIDCounterNeverReusesNumbers",
        "internal/storage/embeddeddolt/child_id_test.go::TestGetNextChildID",
    ],
    "B11": [
        "internal/storage/depid/depid_test.go::TestNewGolden",
        "cmd/bd/protocol/dep_identity_test.go::TestProtocol_DepReAddSameTypeIsIdempotent",
        "cmd/bd/protocol/dep_identity_test.go::TestProtocol_DepReAddDifferentTypeIsAnError",
    ],
    "B12": [
        "cmd/bd/compact_tier2_embedded_test.go::TestEmbeddedAdminCompactTier2Honesty",
        "cmd/bd/restore_embedded_test.go::TestEmbeddedRestore",
        "internal/compact/compactor_test.go::TestCompactTier1_DryRun",
    ],
}


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    data = value if isinstance(value, (bytes, bytearray)) else canonical(value)
    return hashlib.sha256(data).hexdigest()


def verify_source(root: Path) -> dict[str, str]:
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    tree = subprocess.check_output(["git", "-C", str(root), "rev-parse", f"{commit}^{{tree}}"], text=True).strip()
    if commit != PIN or tree != TREE:
        raise RuntimeError(f"source pin mismatch: commit={commit} tree={tree}")
    return {"commit": commit, "tree": tree}


def normalize(value: Any, card: str) -> Any:
    """Remove only declared runtime fields and normalize fresh-workspace noise."""
    if isinstance(value, dict):
        return {
            key: normalize(item, card)
            for key, item in sorted(value.items())
            if key not in NORMALIZED_FIELDS
        }
    if isinstance(value, list):
        return [normalize(item, card) for item in value]
    if isinstance(value, str):
        value = RUNTIME_STAMP.sub("<runtime-time>", value)
        value = WORKSPACE_STAMP.sub("<workspace>", value)
        if card == "B09":
            # The reference hash includes creation time. Keep the observed
            # format and collision facts in the projection, not a made-up ID.
            value = re.sub(r"\bb09-[a-z0-9]{3,8}\b", "<generated-id>", value)
        return value
    return value


class Recorder:
    def __init__(self, binary: Path, source: Path, card: str):
        self.binary = binary.resolve()
        self.source = source.resolve()
        self.card = card
        WORK_ROOT.mkdir(parents=True, exist_ok=True)
        self.workspace = Path(tempfile.mkdtemp(prefix=f"s1-{card.lower()}-", dir=WORK_ROOT))
        self.commands: list[dict[str, Any]] = []
        self.env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("BEADS_", "BD_", "DOLT_"))
        }
        self.env.update({
            "BEADS_ACTOR": f"s1-{card.lower()}-reference",
            "BD_NON_INTERACTIVE": "1",
            "CI": "true",
            "LC_ALL": "C",
            "NO_COLOR": "1",
            "TZ": "UTC",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
        })

    def run(self, label: str, args: list[str], expected: int = 0, stdin: str | None = None) -> tuple[int, bytes, bytes]:
        proc = subprocess.run(
            [str(self.binary), *args], cwd=self.workspace, env=self.env,
            input=stdin, capture_output=True, check=False, text=False, timeout=120,
        )
        stdout = proc.stdout.decode(errors="replace")
        stderr = proc.stderr.decode(errors="replace")
        try:
            stdout_value: Any = normalize(json.loads(stdout), self.card)
        except json.JSONDecodeError:
            stdout_value = normalize(stdout, self.card)
        self.commands.append({
            "label": label,
            "argv": normalize(["reference-engine", *args], self.card),
            "exitCode": proc.returncode,
            "stdout": stdout_value,
            "stderr": normalize(stderr, self.card),
        })
        if proc.returncode != expected:
            raise RuntimeError(
                f"{label}: exit {proc.returncode}, expected {expected}\n"
                f"stdout={stdout}\nstderr={stderr}"
            )
        return proc.returncode, proc.stdout, proc.stderr

    def json(self, label: str, args: list[str], expected: int = 0) -> Any:
        _, stdout, _ = self.run(label, args, expected)
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{label}: expected JSON, got {stdout!r}") from exc

    def create(self, identifier: str, title: str, *extra: str, issue_type: str = "task") -> dict[str, Any]:
        value = self.json(
            f"create-{identifier}",
            ["create", "--id", identifier, "--title", title, "--type", issue_type, "--force", *extra, "--json"],
        )
        if not isinstance(value, dict):
            raise RuntimeError(f"create-{identifier}: unexpected result {value!r}")
        return value

    def init(self) -> None:
        self.run(
            "init", ["init", "--non-interactive", "--skip-hooks", "--skip-agents", "--prefix", self.card.lower(), "--quiet"],
        )

    def card_b09(self) -> dict[str, Any]:
        first = self.json("hash-first", ["create", "--title", "Fix login", "--description", "Details", "--type", "task", "--json"])
        second = self.json("hash-second", ["create", "--title", "Fix login", "--description", "Details", "--type", "task", "--json"])
        first_id = first["id"]
        second_id = second["id"]
        self.run(
            "hash-collision-refusal",
            ["create", "--id", first_id, "--title", "Fix login", "--description", "Details", "--type", "task", "--json"],
            expected=1,
        )
        rows = self.json("hash-list", ["list", "--all", "--limit", "0", "--json"])
        projection = {
            "createdIDsDistinct": first_id != second_id,
            "createdIDPattern": all(bool(re.fullmatch(r"b09-[a-z0-9]{3,8}", value)) for value in (first_id, second_id)),
            "sameSemanticFields": all(
                value.get(field) == first.get(field)
                for value in (second,)
                for field in ("title", "description", "issue_type", "status")
            ),
            "collisionRefused": self.commands[-2]["exitCode"] != 0,
            "rowsAfterCollision": len(rows),
        }
        return {
            "observations": {"first": normalize(first, self.card), "second": normalize(second, self.card), "rows": normalize(rows, self.card)},
            "projection": projection,
            "limits": [
                "The source hash vector is proven by the pinned source test; the CLI's creation timestamp is intentionally omitted.",
                "Collision evidence is public create refusal; no synthetic timestamp or hash collision is claimed.",
            ],
        }

    def card_b10(self) -> dict[str, Any]:
        self.create("b10-parent", "Parent", issue_type="epic")
        explicit = self.create("b10-parent.8", "Imported child")
        child9 = self.json("child-9", ["create", "--parent", "b10-parent", "--title", "Child nine", "--type", "task", "--json"])
        child10 = self.json("child-10", ["create", "--parent", "b10-parent", "--title", "Child ten", "--type", "task", "--json"])
        self.run("delete-10", ["delete", "b10-parent.10", "--force", "--json"])
        child11 = self.json("child-11", ["create", "--parent", "b10-parent", "--title", "Child eleven", "--type", "task", "--json"])
        self.run("close-11", ["close", "b10-parent.11", "--reason", "done", "--json"])
        child12 = self.json("child-12", ["create", "--parent", "b10-parent", "--title", "Child twelve", "--type", "task", "--json"])
        self.run("delete-live-children", ["delete", "b10-parent.8", "b10-parent.9", "b10-parent.11", "b10-parent.12", "--force", "--json"])
        child13 = self.json("child-13", ["create", "--parent", "b10-parent", "--title", "Child thirteen", "--type", "task", "--json"])
        children = self.json("children", ["children", "b10-parent", "--json"])
        ids = [explicit["id"], child9["id"], child10["id"], child11["id"], child12["id"], child13["id"]]
        projection = {
            "explicitHighWater": explicit["id"] == "b10-parent.8",
            "mintedAfterImported": child9["id"] == "b10-parent.9",
            "sequentialContinuation": [child10["id"], child11["id"], child12["id"], child13["id"]] == [
                "b10-parent.10", "b10-parent.11", "b10-parent.12", "b10-parent.13"
            ],
            "noReuseAfterDeleteAndClose": child13["id"] == "b10-parent.13",
            "childRowsAfterCleanup": sorted(row["id"] for row in children) == ["b10-parent.13"],
            "observedIDs": ids,
        }
        return {
            "observations": {"explicit": normalize(explicit, self.card), "children": normalize(children, self.card)},
            "projection": projection,
            "limits": [
                "The public CLI case covers explicit imported high-water and monotonic parent allocation; direct counter-table inspection is source evidence only.",
                "Dotted child IDs are retained; runtime timestamps and Dolt revisions are omitted.",
            ],
        }

    def card_b11(self) -> dict[str, Any]:
        self.create("b11-source", "Source")
        self.create("b11-target", "Target")
        self.json("add-blocks", ["dep", "add", "b11-source", "b11-target", "--type", "blocks", "--json"])
        self.json("readd-blocks", ["dep", "add", "b11-source", "b11-target", "--type", "blocks", "--json"])
        before = self.json("show-before-retype", ["show", "b11-source", "--json"])
        self.run("retype-refusal", ["dep", "add", "b11-source", "b11-target", "--type", "relates-to", "--json"], expected=1)
        after_refusal = self.json("show-after-retype", ["show", "b11-source", "--json"])
        self.json("remove-blocks", ["dep", "remove", "b11-source", "b11-target", "--json"])
        self.json("add-relates", ["dep", "add", "b11-source", "b11-target", "--type", "relates-to", "--json"])
        after_readd = self.json("show-after-readd", ["show", "b11-source", "--json"])
        self.json("remove-relates", ["dep", "remove", "b11-source", "b11-target", "--json"])
        final = self.json("show-final", ["show", "b11-source", "--json"])

        def edge(row: list[dict[str, Any]]) -> tuple[str, str] | None:
            deps = row[0].get("dependencies", []) if row else []
            if not deps:
                return None
            return deps[0].get("id"), deps[0].get("dependency_type")

        exits = {command["label"]: command["exitCode"] for command in self.commands}
        projection = {
            "sameTypeReaddAccepted": exits["readd-blocks"] == 0,
            "typedRetypeRefused": exits["retype-refusal"] != 0,
            "retypeNoWrite": edge(before) == edge(after_refusal) == ("b11-target", "blocks"),
            "unlinkThenDifferentTypeAccepted": edge(after_readd) == ("b11-target", "relates-to"),
            "unlinkConservesEndpoints": final[0].get("dependency_count", 0) == 0,
        }
        return {
            "observations": {
                "before": normalize(before, self.card),
                "afterRefusal": normalize(after_refusal, self.card),
                "afterReadd": normalize(after_readd, self.card),
                "final": normalize(final, self.card),
            },
            "projection": projection,
            "limits": [
                "The public CLI does not expose the stored UUIDv5 edge ID; deterministic depid vectors remain pinned by the source test and Plug package replay.",
                "The projection compares endpoint and typed-edge identity, including the refusal's no-write state.",
            ],
        }

    def card_b12(self) -> dict[str, Any]:
        self.create("b12-source", "Source", "--description", "Original long description for reversible compaction.", "--design", "Original design")
        self.run("close-source", ["close", "b12-source", "--reason", "complete", "--json"])
        analyze = self.json("admin-compact-analyze", ["admin", "compact", "--analyze", "--json"])
        summary = self.workspace / "summary.txt"
        summary.write_text("Summary retained by the agent.\n", encoding="utf-8")
        self.run(
            "admin-compact-apply",
            ["admin", "compact", "--apply", "--id", "b12-source", "--summary", str(summary), "--json"],
            expected=1,
        )
        self.run("restore-read", ["restore", "b12-source", "--json"], expected=1)
        self.run("restore-apply", ["restore", "b12-source", "--apply", "--json"], expected=1)
        projection = {
            "analyzeSucceeded": isinstance(analyze, dict),
            "analyzeCandidateCount": analyze.get("summary", {}).get("total_candidates", -1),
            "applyRefusedInEmbeddedMode": self.commands[-3]["exitCode"] != 0,
            "restoreReadRefusedWithoutSnapshot": self.commands[-2]["exitCode"] != 0,
            "restoreApplyRefusedWithoutSnapshot": self.commands[-1]["exitCode"] != 0,
        }
        return {
            "observations": {"analyze": normalize(analyze, self.card)},
            "projection": projection,
            "limits": [
                "The pinned embedded reference exposes analyze but refuses admin compact apply; restore --apply is exercised and refuses because no source snapshot exists.",
                "Plug adopts permanent diff history plus history.revert for reversible text, so this source refusal is a recorded deviation, not a fabricated success.",
            ],
        }

    def record(self) -> dict[str, Any]:
        subprocess.run(["git", "init", "-q"], cwd=self.workspace, env=self.env, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.name", "s1-b09-b12-reference"],
            cwd=self.workspace, env=self.env, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "s1-b09-b12-reference@invalid"],
            cwd=self.workspace, env=self.env, check=True, capture_output=True,
        )
        self.init()
        card = getattr(self, f"card_{self.card.lower()}")()
        payload = {
            "schema": "plug.s1-b09-b12.beads-reference.v1",
            "card": self.card,
            "engine": "embedded-dolt",
            "sourceCommit": PIN,
            "sourceTree": TREE,
            "binarySha256": digest(self.binary.read_bytes()),
            "commands": self.commands,
            "observations": card["observations"],
            "projection": card["projection"],
            "limits": card["limits"],
        }
        payload["rawDigest"] = digest({"commands": payload["commands"], "observations": payload["observations"]})
        payload["projectionDigest"] = digest(payload["projection"])
        return payload


def source_evidence(source: Path, card: str) -> list[dict[str, str]]:
    result = []
    for item in SOURCE_TESTS[card]:
        relative, symbol = item.split("::", 1)
        path = source / relative
        if not path.is_file():
            raise RuntimeError(f"pinned source file is missing: {relative}")
        text = path.read_text(encoding="utf-8")
        if not re.search(r"\bfunc " + re.escape(symbol) + r"\(", text):
            raise RuntimeError(f"pinned source symbol is missing: {item}")
        result.append({"path": relative, "sha256": digest(path.read_bytes()), "symbol": symbol})
    return result


def schema_evidence(source: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for plane, directory in (
        ("main", source / "internal/storage/schema/migrations"),
        ("ignored", source / "internal/storage/schema/migrations/ignored"),
    ):
        files = [{"path": str(path.relative_to(source)), "sha256": digest(path.read_bytes())} for path in sorted(directory.glob("*.up.sql"))]
        if not files:
            raise RuntimeError(f"empty {plane} migration inventory")
        result[plane] = {"cursor": max(int(Path(item["path"]).name.split("_", 1)[0]) for item in files), "files": len(files), "digest": digest(files)}
    return result


def one_run(card: str, binary: Path, source: Path) -> dict[str, Any]:
    recorder = Recorder(binary, source, card)
    try:
        result = recorder.record()
        result["sourceTests"] = SOURCE_TESTS[card]
        result["sourceRefs"] = source_evidence(source, card)
        return result
    finally:
        shutil.rmtree(recorder.workspace, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("card", choices=tuple(SOURCE_TESTS))
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not args.reference.is_file():
        raise SystemExit("BLOCKED: exact source-built reference executable is unavailable")
    if digest(args.reference.read_bytes()) != BINARY_SHA256:
        raise SystemExit("BLOCKED: reference executable SHA-256 differs from the pinned build")
    source = verify_source(args.source_root)
    first = one_run(args.card, args.reference, args.source_root)
    repeat = one_run(args.card, args.reference, args.source_root)
    if first["projection"] != repeat["projection"]:
        raise SystemExit("BLOCKED: independent source projections differ")
    if first["rawDigest"] != repeat["rawDigest"]:
        raise SystemExit("BLOCKED: normalized independent source transcripts differ")
    recorder_digest = digest(Path(__file__).read_bytes())
    config = {
        "actor": f"s1-{args.card.lower()}-reference",
        "prefix": args.card.lower(),
        "workspace": "fresh-disposable-Git-backed",
        "timezone": "UTC",
        "driver": "embedded-dolt",
    }
    inputs = {
        "card": args.card,
        "sourceTests": SOURCE_TESTS[args.card],
        "normalizedFields": sorted(NORMALIZED_FIELDS),
    }
    output = {
        "schema": "plug.s1-b09-b12.reference-recording.v1",
        "source": {**source, "sourceRefs": first["sourceRefs"]},
        "binary": {"sha256": BINARY_SHA256, "path": str(args.reference.resolve())},
        "driver": {"name": "embedded-dolt", "version": DRIVER},
        "recorder": {"identity": "s1-b09-b12-reference-recorder", "version": "1", "source": "spec/s1-b09-b12/record_reference.py", "sha256": recorder_digest},
        "configuration": {"value": config, "sha256": digest(config)},
        "inputs": {"value": inputs, "sha256": digest(inputs)},
        "schemaEvidence": schema_evidence(args.source_root),
        "card": args.card,
        "first": first,
        "repeat": repeat,
        "projection": first["projection"],
        "projectionSha256": first["projectionDigest"],
        "rawSha256": first["rawDigest"],
        "repeated": {"rawSha256": repeat["rawDigest"], "projectionSha256": repeat["projectionDigest"]},
        "disposition": {
            "status": "deviate" if args.card == "B12" else "keep",
            "scope": "recorded-reference-vector",
            "source": f"engdocs/design/plug/spec/behaviors/{'content' if args.card == 'B12' else 'work'}.md#{args.card}",
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(output) + b"\n")
    print(json.dumps({"card": args.card, "output": str(args.output), "rawSha256": first["rawDigest"], "projectionSha256": first["projectionDigest"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
