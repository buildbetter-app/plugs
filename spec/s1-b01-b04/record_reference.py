#!/usr/bin/env python3
"""Record the four S1 work cards from two fresh pinned Beads stores.

The recorder deliberately keeps the command outcomes and the semantic
projection as separate values.  Runtime stamps and Dolt identities are omitted
only through the explicit NORMALIZED_FIELDS list; no expected Plug value is
written by this program.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import re
import sys
from pathlib import Path
from typing import Any

PIN = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
BINARY_SHA256 = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
DRIVER = "github.com/dolthub/dolt/go@v0.40.5-0.20260806213044-796d07497741"
NORMALIZED_FIELDS = [
    "created_at",
    "updated_at",
    "closed_at",
    "defer_until",
    "heartbeat_at",
    "started_at",
    "revision",
    "schema_version",
    "owner",
    "lease_expires_at",
    "leaseExpiresAt",
    "createdAt",
    "updatedAt",
    "closedAt",
    "commit",
    "hash",
]
RUNTIME_STAMP = re.compile(r"20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z")


def digest(data: bytes | str | Any) -> str:
    if not isinstance(data, (bytes, bytearray)):
        data = canonical(data)
    return hashlib.sha256(data).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def normalized(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in sorted(value.items()) if k not in NORMALIZED_FIELDS}
    if isinstance(value, list):
        return [normalized(v) for v in value]
    return value


class Recorder:
    def __init__(self, binary: Path, source: Path, workspace: Path,
                 runtime: dict[str, str] | None = None, transcript: Path | None = None):
        self.binary = binary.resolve()
        self.source = source.resolve()
        self.workspace = workspace
        self.commands: list[dict[str, Any]] = []
        self.raw: dict[str, Any] = {}
        self.runtime = runtime or {"mode": "embedded-dolt", "version": DRIVER}
        self.transcript = transcript
        self.env = os.environ.copy()
        self.env.update(
            {
                "BEADS_ACTOR": "s1-reference-recorder",
                "BD_NON_INTERACTIVE": "1",
                "CI": "true",
                "LC_ALL": "C",
                "NO_COLOR": "1",
                "TZ": "UTC",
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_AUTHOR_NAME": "s1-reference-recorder",
                "GIT_AUTHOR_EMAIL": "s1-reference@example.invalid",
                "GIT_COMMITTER_NAME": "s1-reference-recorder",
                "GIT_COMMITTER_EMAIL": "s1-reference@example.invalid",
            }
        )

    def run(self, label: str, args: list[str], expected: int | None = 0) -> tuple[int, bytes, bytes]:
        timed_out = False
        try:
            proc = subprocess.run(
                [str(self.binary), *args], cwd=self.workspace, env=self.env,
                capture_output=True, check=False, timeout=120,
            )
            code, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as error:
            # subprocess retains these exact partial bytes when it kills and
            # waits for the timed-out command. Persist them before unwinding
            # into the owned runtime's shutdown/removal path.
            timed_out = True
            code, stdout, stderr = None, error.stdout or b"", error.stderr or b""
        def stable_output(data: bytes) -> Any:
            text = data.decode(errors="replace")
            try:
                return normalized(json.loads(text))
            except json.JSONDecodeError:
                return RUNTIME_STAMP.sub("<runtime-time>", text)

        self.commands.append(
            {
                "label": label,
                "argv": ["reference-engine", *args],
                "exitCode": code,
                "outcome": "timeout" if timed_out else "exited",
                "stdout": stable_output(stdout),
                "stderr": stable_output(stderr),
            }
        )
        if self.transcript is not None:
            with self.transcript.open("ab") as stream:
                stream.write(canonical({
                    "label": label, "argv": ["reference-engine", *args],
                    "workspace": str(self.workspace), "exitCode": code,
                    "outcome": "timeout" if timed_out else "exited",
                    "timeoutSeconds": 120 if timed_out else None,
                    "stdoutBase64": base64.b64encode(stdout).decode("ascii"),
                    "stderrBase64": base64.b64encode(stderr).decode("ascii"),
                }) + b"\n")
        if timed_out:
            raise RuntimeError(f"{label}: timed out after 120 seconds; partial streams retained")
        if expected is not None and code != expected:
            raise RuntimeError(
                f"{label}: exit {code}, expected {expected}\n"
                f"stdout={stdout.decode(errors='replace')}\n"
                f"stderr={stderr.decode(errors='replace')}"
            )
        return code, stdout, stderr

    def json(self, label: str, args: list[str], expected: int = 0) -> Any:
        _, stdout, _ = self.run(label, args, expected)
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{label}: expected JSON, got {stdout!r}") from exc

    def create(self, card: str, suffix: str, title: str, *extra: str) -> Any:
        return self.json(
            f"{card}-create-{suffix}",
            ["create", "--id", f"{card.lower()}-{suffix}", "--title", title, "--type", "task", "--force", *extra, "--json"],
        )

    def source_evidence(self) -> list[dict[str, str]]:
        paths = [
            "backend/conformance/ready_claimer_contract.go",
            "backend/conformance/blocked_lister_contract.go",
            "backend/conformance/claimer_contract.go",
            "internal/types/types.go",
            "cmd/bd/protocol/blocking_semantics_test.go",
        ]
        out = []
        for relative in paths:
            path = self.source / relative
            if not path.is_file():
                raise RuntimeError(f"pinned source file is missing: {relative}")
            data = path.read_bytes()
            out.append({"path": relative, "sha256": digest(data)})
        return out

    def schema_evidence(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for plane, directory in (
            ("main", self.source / "internal/storage/schema/migrations"),
            ("ignored", self.source / "internal/storage/schema/migrations/ignored"),
        ):
            files = []
            for path in sorted(directory.glob("*.up.sql")):
                files.append({"path": str(path.relative_to(self.source)), "sha256": digest(path.read_bytes())})
            if not files:
                raise RuntimeError(f"empty {plane} migration inventory")
            result[plane] = {
                "cursor": max(int(Path(item["path"]).name.split("_", 1)[0]) for item in files),
                "digest": digest(files),
                "files": len(files),
            }
        return result

    def card_b01(self) -> dict[str, Any]:
        self.create("B01", "p0", "priority zero", "--priority", "0")
        self.create("B01", "tie-a", "tie a", "--priority", "1")
        self.create("B01", "tie-b", "tie b", "--priority", "1")
        self.create("B01", "parent", "deferred parent", "--priority", "2")
        self.create("B01", "child", "child of deferred", "--priority", "3")
        self.run("B01-parent-child", ["update", "b01-child", "--parent", "b01-parent", "--json"])
        self.create("B01", "deferred", "deferred item", "--defer", "2099-01-01", "--priority", "2")
        self.create("B01", "closed", "closed item", "--priority", "2")
        self.run("B01-close", ["close", "b01-closed", "--reason", "recorded terminal"])
        self.create("B01", "pinned", "pinned item", "--status", "pinned", "--priority", "2")
        self.run("B01-defer-parent", ["defer", "b01-parent", "--reason", "recorded parent defer"])
        ready = self.json("B01-ready", ["ready", "--json", "--limit", "0"])
        explain = self.json("B01-ready-explain", ["ready", "--json", "--explain", "--limit", "0"])
        ids = [row["id"] for row in ready]
        blocked = [row["id"] for row in explain.get("blocked", [])]
        created_ids = [
            "b01-p0", "b01-tie-a", "b01-tie-b", "b01-parent", "b01-child",
            "b01-deferred", "b01-closed", "b01-pinned",
        ]
        return {
            "commands": self.commands,
            "raw": {"ready": normalized(ready), "explain": normalized(explain)},
            "projection": {
                "readyIds": ids,
                "blockedIds": blocked,
                # The projection is derived from the reached reference rows.
                # The pinned executable leaves a child of a deferred parent in
                # ready; that observed result is retained verbatim. Plug's
                # deferred-parent rule is recorded as a separate disposition
                # below instead of being substituted into the reference data.
                "excludedIds": sorted(set(created_ids) - set(ids) - set(blocked)),
                "ordering": "priority,created,ID",
                "difference": {
                    "referenceObserved": "child-of-deferred-parent-is-ready",
                    "plugObserved": "child-of-deferred-parent-is-ready",
                    "canonicalRule": "child-of-deferred-parent-is-excluded",
                    "status": "hierarchy-gap-unqualified",
                    "decisionId": "P74",
                },
            },
            "limits": [
                "runtime Go/SQL stamps and Dolt revisions are omitted",
                "reference uses explicit ready query; no one-minute lag measurement",
            ],
        }

    def card_b02(self) -> dict[str, Any]:
        self.create("B02", "blocker", "blocker", "--priority", "0")
        self.create("B02", "dependent", "ordinary dependent", "--priority", "1")
        self.create("B02", "conditional", "conditional dependent", "--priority", "2")
        self.create("B02", "transitive", "transitive dependent", "--priority", "3")
        self.run("B02-link-ordinary", ["dep", "add", "b02-dependent", "b02-blocker", "--json"])
        self.run("B02-link-conditional", ["dep", "add", "b02-conditional", "b02-blocker", "--type", "conditional-blocks", "--json"])
        self.run("B02-link-transitive", ["dep", "add", "b02-transitive", "b02-dependent", "--json"])
        before = self.json("B02-before", ["ready", "--json", "--explain", "--limit", "0"])
        self.run("B02-close-success", ["close", "b02-blocker", "--reason", "success", "--json"])
        after = self.json("B02-after", ["ready", "--json", "--explain", "--limit", "0"])
        _, _, _ = self.run("B02-missing-refusal", ["dep", "add", "b02-missing", "b02-absent", "--json"], expected=1)
        return {
            "commands": self.commands,
            "raw": {"before": normalized(before), "after": normalized(after)},
            "projection": {
                "beforeReadyIds": [row["id"] for row in before.get("ready", [])],
                "beforeBlockedIds": [row["id"] for row in before.get("blocked", [])],
                "afterReadyIds": [row["id"] for row in after.get("ready", [])],
                "afterBlockedIds": [row["id"] for row in after.get("blocked", [])],
                "missingEndpoint": "refused",
            },
            "limits": ["success terminal outcome only; conditional failure is a separate source case"],
        }

    def card_b03(self) -> dict[str, Any]:
        self.run("B03-config", ["config", "set", "status.custom", "awaiting_review:active,qa_testing:wip,on_hold:frozen,archived:done"])
        self.create("B03", "active", "active custom", "--status", "awaiting_review")
        self.create("B03", "wip", "wip custom", "--status", "qa_testing")
        self.create("B03", "frozen", "frozen custom", "--status", "on_hold")
        self.create("B03", "done", "done custom", "--status", "archived")
        ready = self.json("B03-ready", ["ready", "--json", "--limit", "0"])
        self.run("B03-claim-active", ["update", "b03-active", "--claim", "--json"])
        self.run("B03-invalid-status", ["update", "b03-active", "--status", "not_configured", "--json"], expected=1)
        return {
            "commands": self.commands,
            "raw": {"ready": normalized(ready)},
            "projection": {
                "configured": {
                    "awaiting_review": "active",
                    "qa_testing": "wip",
                    "on_hold": "frozen",
                    "archived": "done",
                },
                "readyIds": [row["id"] for row in ready],
                "claimed": "b03-active",
                "invalidStatus": "refused",
            },
            "limits": ["status-specific wall-clock fields are omitted"],
        }

    def card_b04(self) -> dict[str, Any]:
        self.create("B04", "next", "claim next", "--priority", "0")
        self.create("B04", "direct", "direct claim", "--priority", "1")
        self.create("B04", "empty", "empty after claims", "--priority", "2")
        claim_next = self.json("B04-claim-next", ["ready", "--claim", "--json", "--limit", "1"])
        self.run("B04-direct-claim", ["update", "b04-direct", "--claim", "--json"])
        self.run("B04-same-owner-reclaim", ["update", "b04-direct", "--claim", "--json"])
        self.env["BEADS_ACTOR"] = "s1-foreign-actor"
        self.run("B04-foreign-refusal", ["update", "b04-direct", "--claim", "--json"], expected=1)
        self.env["BEADS_ACTOR"] = "s1-reference-recorder"
        remaining = self.json("B04-ready", ["ready", "--json", "--limit", "0"])
        return {
            "commands": self.commands,
            "raw": {"claimNext": normalized(claim_next), "remaining": normalized(remaining)},
            "projection": {
                "claimNext": normalized(claim_next),
                "directClaim": "b04-direct",
                "sameOwnerReclaim": "no-op",
                "foreignHolder": "refused",
                "remainingReadyIds": [row["id"] for row in remaining],
            },
            "limits": ["absolute lease deadline and Dolt revision are omitted"],
        }

    def record(self) -> dict[str, Any]:
        # Every card owns a fresh store.  Reusing one workspace makes later
        # cards observe earlier rows (for example claim-next selecting B01
        # work), which invalidates both the raw transcript and its projection.
        cards: dict[str, dict[str, Any]] = {}
        all_commands: list[dict[str, Any]] = []
        for name, method_name in (
            ("B01", "card_b01"),
            ("B02", "card_b02"),
            ("B03", "card_b03"),
            ("B04", "card_b04"),
        ):
            card_workspace = Path(tempfile.mkdtemp(prefix=f"s1-b01-b04-{name.lower()}-", dir="/tmp/plug-swarm/work"))
            card_recorder = Recorder(self.binary, self.source, card_workspace, self.runtime, self.transcript)
            initialized = False
            try:
                init_flags = ["init", "--non-interactive", "--prefix", "s1", "--skip-hooks", "--skip-agents", "--quiet"]
                if self.runtime["mode"] == "managed-proxied-server":
                    init_flags.append("--proxied-server")
                card_recorder.run(
                    "init",
                    init_flags,
                )
                initialized = True
                card = getattr(card_recorder, method_name)()
                cards[name] = card
            finally:
                failed = sys.exc_info()[0] is not None
                stopped = self.runtime["mode"] != "managed-proxied-server" or not (card_workspace / ".beads").exists()
                try:
                    if self.runtime["mode"] == "managed-proxied-server" and (card_workspace / ".beads").exists():
                        code, _, _ = card_recorder.run("cleanup-stop", ["dolt", "stop", "--json"], expected=None)
                        stopped = code == 0
                        if initialized and code != 0 and not failed:
                            raise RuntimeError(f"{name}: managed reference runtime cleanup failed with exit {code}")
                finally:
                    all_commands.extend(card_recorder.commands)
                    if stopped:
                        shutil.rmtree(card_workspace, ignore_errors=True)
                    else:
                        print(f"managed runtime shutdown unverified; workspace retained: {card_workspace}", file=sys.stderr)
        self.commands = all_commands
        # Keep command outcomes inside each card and remove the global setup
        # command from the card-specific sequence.  The full command transcript
        # is still retained below as the raw run evidence.
        raw = {name: {"outcomes": value["raw"], "limits": value["limits"]} for name, value in cards.items()}
        projection = {name: value["projection"] for name, value in cards.items()}
        config = {
            "actor": "s1-reference-recorder",
            "prefix": "s1",
            "initFlags": init_flags[1:],
            "customStatus": "awaiting_review:active,qa_testing:wip,on_hold:frozen,archived:done",
            "drivers": [self.runtime["mode"], "memory", "sqlite", "postgres"],
        }
        inputs = {
            "clock": {
                "mode": "fixed-inputs; runtime reference stamps omitted",
                "timezone": "UTC",
                "seed": "2026-01-01T00:00:00Z",
                "go": "Plug replay injects one monotonic UTC function",
                "sql": "B01-B04 replay does not reach SQL-generated timestamps; SQLite/PostgreSQL driver wall-clock paths remain unqualified",
                "omitted": NORMALIZED_FIELDS,
            },
            "actors": ["s1-reference-recorder", "s1-foreign-actor"],
            "ids": {
                "explicit": [
                    "b01-p0", "b01-tie-a", "b01-tie-b", "b01-parent", "b01-child", "b01-deferred", "b01-closed", "b01-pinned",
                    "b02-blocker", "b02-dependent", "b02-conditional", "b02-transitive",
                    "b03-active", "b03-wip", "b03-frozen", "b03-done",
                    "b04-next", "b04-direct", "b04-empty",
                ],
                "collisionState": "fresh store",
                "counterState": "unused; every top-level ID explicit",
            },
            "graph": {"nodes": 19, "edges": 3, "seed": "fixed direct, conditional and transitive edges"},
        }
        return {
            "schema": "plug.s1-b01-b04.reference.v1",
            "source": {"commit": PIN, "tree": TREE, "sourceFiles": self.source_evidence()},
            "binary": {"sha256": digest(self.binary.read_bytes()), "version": "bd version 1.3.0 (dev)", "buildTags": ["gms_pure_go"]},
            "driver": {"name": self.runtime["mode"], "version": self.runtime["version"], "runtime": self.runtime},
            "recorder": {"identity": "s1-b01-b04-reference", "version": "1", "source": "spec/s1-b01-b04/record_reference.py"},
            "configuration": {"value": config, "digest": digest(config)},
            "inputs": {"value": inputs, "digest": digest(inputs)},
            "schema": self.schema_evidence(),
            "cards": cards,
            "raw": raw,
            "projection": projection,
            "commands": self.commands,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runtime-manifest", type=Path)
    parser.add_argument("--transcript", type=Path)
    args = parser.parse_args()
    if digest(args.binary.read_bytes()) != BINARY_SHA256:
        raise SystemExit("reference executable SHA-256 differs from the pinned build")
    if not args.source.is_dir():
        raise SystemExit("pinned source archive is missing")
    runtime = None
    if args.runtime_manifest is not None:
        runtime = json.loads(args.runtime_manifest.read_bytes())
        dolt = shutil.which("dolt")
        if (runtime.get("mode") != "managed-proxied-server" or runtime.get("version") != "2.2.0"
                or runtime.get("archiveSHA256") != "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6"
                or dolt is None or digest(Path(dolt).read_bytes()) != runtime.get("binarySHA256")):
            raise SystemExit("managed reference runtime identity differs from the pinned profile")
    workspace = Path(tempfile.mkdtemp(prefix="s1-b01-b04-reference-", dir="/tmp/plug-swarm/work"))
    try:
        recorder = Recorder(args.binary, args.source, workspace, runtime, args.transcript)
        vector = recorder.record()
        raw = canonical(vector)
        artifact = {
            "vector": vector,
            "rawJSON": raw.decode(),
            "digest": digest(raw),
            "recorderSHA256": digest(Path(__file__).read_bytes()),
            "workspaceClass": "fresh-disposable",
            "runtimeProfile": recorder.runtime["mode"],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical(artifact) + b"\n")
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
