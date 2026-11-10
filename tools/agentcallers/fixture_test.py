#!/usr/bin/env python3
"""Synthetic end-to-end gate for the prepared hook and catch-up adapters.

The server is deliberately local and in-memory. It enforces the inspected
typed-RPC surface: server.hello is metadata-free, every other request has
explicit project/agent metadata, identity comes from agent.brief, and the
catalog-only auth.whoami method is rejected. The view fixture enforces the
pinned unfiltered view grammar, supported fields, revision sort, and bead.get claim
read used to recover the legacy assignee. No host store, credentials, or
private caller data are read.
"""

from __future__ import annotations

import copy
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from typing import Any


ROOT = Path(__file__).resolve().parent
HOOK = ROOT / "hook_adapter.py"
CATCH_UP = ROOT / "catch_me_up_adapter.py"
INCARNATION = "fixture-incarnation-20261105"
AS_OF = {"revision": 700, "incarnation": INCARNATION}
AGENT = "fixture-agent"
PRINCIPAL = {"kind": "user", "id": "user:fixture-user"}
PROJECTS = ("cody-ops", "signals-v2")
EVENTS = ("PostCompact", "PreCompact", "SessionStart", "UserPromptSubmit")
FIELDS = ["id", "type", "project", "status", "fields", "metadata", "labels", "revision"]


def _ontology(project):
    categories = {"open": "active", "todo": "active", "in_progress": "wip", "blocked": "frozen", "closed": "done",
                  "queued": "active" if project == "cody-ops" else "done"}
    return {"revision": 9, "drifted": False, "problems": [], "document": {"statuses": {
        name: {"name": name.title(), "category": category} for name, category in categories.items()
    }}}


def _claim_row(project: str, ident: str, *, status: str, reason: str | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {"id": ident, "title": f"{status} claim for {project}", "status": status, "metadata": {}}
    if reason is not None:
        row["metadata"]["i8.host.close_reason"] = {"schema": "plug-host-close-reason/1", "reason": reason}
    return row


def _event_position(project: str, index: int) -> dict[str, Any]:
    return {"cursor": f"{project}-event-{index}", "incarnation": INCARNATION, "revision": 600 + index, "index": index}


def _durable_event(project: str, index: int, event_type: str, resource: str, before: dict[str, Any], after: dict[str, Any], changed: list[str]) -> dict[str, Any]:
    return {
        "category": "durable",
        **_event_position(project, index),
        "type": event_type,
        "resource": resource,
        "changed": changed,
        "before": before,
        "after": after,
        "principal": copy.deepcopy(PRINCIPAL),
        "chain": [],
        "setBy": {},
        "cause": {"root": f"{project}-root", "depth": 0, "chain": []},
        "derived": False,
    }


def _brief(project: str) -> dict[str, Any]:
    typed_reason = {"schema": "plug-host-close-reason/1", "reason": "synthetic completed"}
    close_after = {"status": "closed", "metadata": {"i8.host.close_reason": typed_reason}}
    reopen_after = {"status": "open", "metadata": {}}
    return {
        "whoami": {"principal": copy.deepcopy(PRINCIPAL)},
        "claims": [
            _claim_row(project, f"{project}-closed-claim", status="closed", reason="synthetic completed"),
            _claim_row(project, f"{project}-reopened-claim", status="open"),
        ],
        "readyTop": [{"id": f"{project}-ready", "title": f"ready for {project}", "status": "open", "metadata": {}}],
        "recentChanges": [
            _durable_event(
                project,
                1,
                "bead.closed",
                f"bead/{project}-closed",
                {"status": "open", "metadata": {}},
                close_after,
                ["status", "metadata"],
            ),
            _durable_event(
                project,
                2,
                "bead.reopened",
                f"bead/{project}-closed",
                close_after,
                reopen_after,
                ["status", "metadata"],
            ),
            {
                "category": "control",
                **_event_position(project, 3),
                "type": "event.gap",
                "details": {
                    "fromIncarnation": "previous-incarnation",
                    "toIncarnation": INCARNATION,
                    "floor": 599,
                    "reason": "restore",
                },
                "derived": False,
            },
        ],
        "ontologySummary": {
            "project": project,
            "statusMap": {"open": "unstarted", "in_progress": "started", "blocked": "backlog", "closed": "completed"},
            "typeMap": {"task": "task", "decision": "decision"},
        },
        "conventions": {"project": project, "fixture": "synthetic"},
        "memories": [{"key": f"memory:{project}", "content": f"memory for {project}"}],
    }


def _row(project: str, ident: str, *, status: str, kind: str, revision: int, description: Any = ...):
    fields: dict[str, Any] = {"title": f"title {ident}", "priority": 1}
    if description is not ...:
        fields["description"] = description
    return {
        "id": ident,
        "project": project,
        "status": status,
        "type": kind,
        "revision": revision,
        "fields": fields,
        "metadata": {"source": "synthetic", "selected": kind in {"decision", "choice"} or ident.endswith("-nikhil")},
        "labels": [f"label:{project}"],
    }


def _project_rows(project: str) -> tuple[dict[str, dict[str, Any]], dict[str, list[list[dict[str, Any]]]]]:
    nikhil = f"{project}-nikhil"
    other = f"{project}-other"
    waiting = f"{project}-waiting"
    decision = f"{project}-decision"
    null_decision = f"{project}-null-decision"
    current_assigned = f"{project}-writable"
    reopened = f"{project}-reopened"
    rows = {
        # The first row is a native/workflow bead. Its live assignment is the
        # separate bead.get lease, with no synthetic bead.assignee member.
        nikhil: _row(project, nikhil, status="open", kind="work_item", revision=101, description=f"description {nikhil}"),
        # The remaining rows model restore output: Plug's target type is
        # remapped, while the original fields stay in beads.source_fields.
        other: _row(project, other, status="open", kind="work_item", revision=102),
        waiting: _row(project, waiting, status="in_progress", kind="work_item", revision=103, description=None),
        decision: _row(project, decision, status="blocked", kind="choice", revision=104, description=None),
        null_decision: _row(project, null_decision, status="blocked", kind="choice", revision=105, description=None),
        current_assigned: _row(project, current_assigned, status="todo", kind="work_item", revision=106),
        reopened: _row(project, reopened, status="queued", kind="choice", revision=107),
    }
    rows[current_assigned]["fields"]["assignee"] = "nikhil"
    rows[decision]["fields"]["assignee"] = "nikhil"
    rows[waiting]["fields"]["assignee"] = None
    for ident in (waiting, decision, null_decision):
        # restore.go omits a null description from writable Plug fields; the
        # retained source field map still distinguishes it from omission.
        rows[ident]["fields"].pop("description", None)

    source_fields = {
        other: {"status": "open", "issue_type": "task", "assignee": "someone-else", "title": f"title {other}", "priority": 1, "created_at": "2026-11-05T00:00:02Z", "updated_at": "2026-11-05T01:00:02Z", "metadata": {"selected": False, "source": "synthetic"}},
        waiting: {"status": "in_progress", "issue_type": "task", "assignee": None, "title": f"title {waiting}", "description": None, "priority": 1, "created_at": "2026-11-05T00:00:03Z", "updated_at": "2026-11-05T01:00:03Z", "metadata": {"selected": False, "source": "synthetic"}},
        decision: {"status": "blocked", "issue_type": "decision", "assignee": "legacy-owner", "title": f"title {decision}", "description": None, "priority": 1, "created_at": "2026-11-05T00:00:04Z", "updated_at": "2026-11-05T01:00:04Z", "metadata": {"selected": True, "source": "synthetic"}},
        null_decision: {"status": "blocked", "issue_type": "decision", "assignee": None, "title": f"title {null_decision}", "description": None, "priority": 1, "created_at": "2026-11-05T00:00:05Z", "updated_at": "2026-11-05T01:00:05Z", "metadata": {"selected": True, "source": "synthetic"}},
    }
    source_fields[waiting]["assignee"] = "nikhil"
    source_fields[decision].update(status="closed", closed_at="2025-01-01", close_reason="must not resurrect")
    source_fields[reopened] = {"status": "closed", "issue_type": "decision", "assignee": None,
                               "closed_at": "2025-01-01", "close_reason": "must not resurrect"}
    details: dict[str, dict[str, Any]] = {}
    for ident, view_row in rows.items():
        bead = copy.deepcopy(view_row)
        bead["createdAt"] = f"2026-11-05T00:00:{view_row['revision'] - 100:02d}Z"
        if ident in (nikhil, current_assigned):
            if ident == current_assigned:
                details[ident] = {"bead": bead}
                continue
            bead["fields"]["owner"] = None
            details[ident] = {
                "bead": bead,
                "lease": {"holder": "user:nikhil", "expiresAt": "2026-11-05T02:00:00Z", "ttlMs": 3600000, "epoch": 7},
            }
            continue
        retained = source_fields[ident]
        bead["metadata"].update({
            "beads.source_fields": copy.deepcopy(retained),
            "plug.restore.allocator": {"prefix": "fixture", "lastId": 99},
        })
        for key in ("status", "issue_type", "assignee", "created_at", "updated_at"):
            if key in retained:
                bead["metadata"][f"beads.source.{key}"] = copy.deepcopy(retained[key])
        details[ident] = {"bead": bead}
    pages: dict[str, list[list[dict[str, Any]]]] = {}
    all_rows = sorted(copy.deepcopy(list(rows.values())), key=lambda value: (value["revision"], value["id"]))
    pages["all"] = [all_rows[index:index + 2] for index in range(0, len(all_rows), 2)]
    return details, pages


class _FixtureState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []
        self.failures: list[str] = []
        self.auth_whoami_calls = 0
        self.inject_stale_brief = False
        self.unknown_status = False
        self.unknown_category = False
        self.ontology_drift = False
        self.ontology_reads = 0
        self.rows: dict[str, dict[str, dict[str, Any]]] = {}
        self.pages: dict[str, dict[str, list[list[dict[str, Any]]]]] = {}
        for project in PROJECTS:
            self.rows[project], self.pages[project] = _project_rows(project)

    def record(self, method: str, params: dict[str, Any]) -> None:
        with self.lock:
            self.requests.append({"method": method, "params": copy.deepcopy(params)})

    def fail(self, message: str) -> None:
        with self.lock:
            self.failures.append(message)


class _Handler(BaseHTTPRequestHandler):
    server: "_Server"

    def log_message(self, *_args: Any) -> None:
        return

    def _reply(self, request_id: Any, *, result: dict[str, Any] | None = None, code: str | None = None) -> None:
        if code is None:
            envelope = {"jsonrpc": "2.0", "id": request_id, "result": result}
        else:
            envelope = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": "fixture rejection", "data": {"code": code}},
            }
        raw = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler name
        try:
            if self.headers.get_all("Plug-RPC") != ["1"] or self.headers.get("Accept") != "application/json":
                self.server.state.fail("HTTP caller omitted the actual admission headers")
                self._reply(None, code="forbidden")
                return
            if self.path != "/rpc/v1":
                self._reply(None, code="wrong_path")
                return
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length))
            request_id = request.get("id")
            method = request.get("method")
            params = request.get("params")
            if request.get("jsonrpc") != "2.0" or not isinstance(method, str) or not isinstance(params, dict):
                self._reply(request_id, code="bad_request")
                return
            self.server.state.record(method, params)
            if method == "server.hello":
                if "_meta" in params:
                    self.server.state.fail("server.hello received forbidden _meta")
                    self._reply(request_id, code="hello_meta_forbidden")
                    return
                self._reply(request_id, result={"incarnation": INCARNATION})
                return
            if method == "auth.whoami":
                self.server.state.auth_whoami_calls += 1
                self._reply(request_id, code="method_not_found")
                return
            meta = params.get("_meta")
            if not isinstance(meta, dict) or set(meta) != {"project", "agent"} or meta.get("agent") != AGENT:
                self.server.state.fail(f"invalid metadata for {method}: {meta!r}")
                self._reply(request_id, code="metadata_required")
                return
            project = meta.get("project")
            if project not in PROJECTS:
                self.server.state.fail(f"unknown project in metadata: {project!r}")
                self._reply(request_id, code="unknown_project")
                return
            if method == "ontology.get":
                assert params.get("project") == project
                result = _ontology(project)
                self.server.state.ontology_reads += 1
                if self.server.state.unknown_status:
                    result["document"]["statuses"].pop("queued")
                if self.server.state.unknown_category:
                    result["document"]["statuses"]["queued"]["category"] = "not-a-category"
                if self.server.state.ontology_drift:
                    result["revision"] += self.server.state.ontology_reads
                self._reply(request_id, result=result)
                return
            if method == "agent.brief":
                if params.get("project") != project:
                    self.server.state.fail("agent.brief project argument disagrees with metadata")
                    self._reply(request_id, code="project_mismatch")
                    return
                result = _brief(project)
                if self.server.state.inject_stale_brief:
                    result["claims"].append(
                        {
                            "id": f"{project}-stale-reopened",
                            "title": "stale resolution",
                            "status": "open",
                            "metadata": {
                                "i8.host.close_reason": {
                                    "schema": "plug-host-close-reason/1",
                                    "reason": "must be omitted after reopen",
                                }
                            },
                        }
                    )
                self._reply(request_id, result=result)
                return
            if method == "view.run":
                self._view(request_id, project, params)
                return
            if method == "bead.get":
                self._get(request_id, project, params)
                return
            self.server.state.fail(f"unexpected RPC method: {method}")
            self._reply(request_id, code="unexpected_method")
        except Exception as exc:  # Keep failures visible to the subprocess and summary.
            self.server.state.fail(f"fixture handler exception: {type(exc).__name__}: {exc}")
            self._reply(None, code="fixture_exception")

    def _view(self, request_id: Any, project: str, params: dict[str, Any]) -> None:
        definition = params.get("definition")
        if not isinstance(definition, dict):
            self.server.state.fail("view.run definition is not an object")
            self._reply(request_id, code="view_definition")
            return
        filter_value = definition.get("filter")
        if filter_value != {}:
            self.server.state.fail(f"view.run must not fix status names: {filter_value!r}")
            self._reply(request_id, code="view_query")
            return
        if definition.get("fields") != FIELDS:
            self.server.state.fail(f"view.run used rejected fields: {definition.get('fields')!r}")
            self._reply(request_id, code="view_fields")
            return
        if definition.get("sort") != ["revision"]:
            self.server.state.fail(f"view.run used rejected sort: {definition.get('sort')!r}")
            self._reply(request_id, code="view_sort")
            return
        supplied_as_of = params.get("asOf")
        if supplied_as_of is not None and supplied_as_of != AS_OF:
            self.server.state.fail("view.run changed its asOf snapshot")
            self._reply(request_id, code="view_cursor")
            return
        pages = self.server.state.pages[project]["all"]
        cursor = params.get("cursor")
        if cursor is None:
            page_index = 0
        elif isinstance(cursor, str) and cursor.startswith(f"{project}-all-page-"):
            page_index = int(cursor.rsplit("-", 1)[1]) - 1
        else:
            self.server.state.fail(f"view.run cursor is not valid: {cursor!r}")
            self._reply(request_id, code="view_cursor")
            return
        if page_index >= len(pages):
            self.server.state.fail("view.run cursor exceeded its page chain")
            self._reply(request_id, code="view_cursor")
            return
        result: dict[str, Any] = {"asOf": copy.deepcopy(AS_OF), "items": pages[page_index]}
        if page_index + 1 < len(pages):
            result["nextCursor"] = f"{project}-all-page-{page_index + 2}"
        self._reply(request_id, result=result)

    def _get(self, request_id: Any, project: str, params: dict[str, Any]) -> None:
        requested = params.get("id")
        if not isinstance(requested, dict) or set(requested) != {"id"} or not isinstance(requested["id"], str):
            self.server.state.fail(f"bead.get used an invalid ref: {requested!r}")
            self._reply(request_id, code="get_ref")
            return
        if params.get("asOf") != AS_OF:
            self.server.state.fail("bead.get did not preserve the view asOf snapshot")
            self._reply(request_id, code="get_as_of")
            return
        ident = requested["id"]
        row = self.server.state.rows[project].get(ident)
        if row is None:
            self.server.state.fail(f"bead.get used an unknown project row: {ident!r}")
            self._reply(request_id, code="get_row")
            return
        result = {"bead": copy.deepcopy(row["bead"]), "attachments": []}
        if "lease" in row:
            result["lease"] = copy.deepcopy(row["lease"])
        self._reply(request_id, result=result)


class _Server(ThreadingHTTPServer):
    def __init__(self) -> None:
        self.state = _FixtureState()
        super().__init__(("127.0.0.1", 0), _Handler)


def _run(
    script: Path,
    binding: Path,
    project: str,
    *extra: str,
    stdin: dict[str, Any] | None = None,
    raw_stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["BEADS_DIR"] = "/synthetic/ambient-must-not-be-read"
    env["PLUG_SERVER"] = "http://127.0.0.1:1"
    payload = raw_stdin if raw_stdin is not None else (None if stdin is None else json.dumps(stdin))
    return subprocess.run(
        [sys.executable, str(script), "--binding", str(binding), "--project", project, *extra],
        input=payload,
        text=True,
        capture_output=True,
        cwd=binding.parent,
        env=env,
        timeout=30,
        check=False,
    )


def _canonical_payload(provider: str, event: str, project: str) -> dict[str, Any]:
    if provider == "claude":
        return {
            "hook_event_name": event,
            "session_id": f"claude-{project}",
            "transcript_path": f"/tmp/synthetic/{project}/transcript.jsonl",
            "cwd": f"/tmp/synthetic/{project}",
            "source": "startup",
            "model": "synthetic-model",
            "permission_mode": "default",
        }
    return {
        "hookEventName": event,
        "sessionId": f"codex-{project}",
        "cwd": f"/tmp/synthetic/{project}",
        "transcriptPath": f"/tmp/synthetic/{project}/events.jsonl",
        "reason": "synthetic",
        "model": "synthetic-model",
    }


def _assert_hook(proc: subprocess.CompletedProcess[str], event: str, project: str) -> str:
    assert proc.returncode == 0, f"{event}/{project} failed: {proc.stderr}"
    assert proc.stderr == "", f"unexpected hook stderr: {proc.stderr!r}"
    output = json.loads(proc.stdout)
    assert set(output) == {"hookSpecificOutput"}
    envelope = output["hookSpecificOutput"]
    assert set(envelope) == {"hookEventName", "additionalContext"}
    assert envelope["hookEventName"] == event
    context = envelope["additionalContext"]
    assert isinstance(context, str) and context.endswith("\n")
    assert f"# Plug context ({project})" in context
    assert f"memory:{project}" in context and f"memory for {project}" in context
    assert "## Ontology summary" in context and f'"project": "{project}"' in context
    assert f"durable type=bead.closed resource=bead/{project}-closed" in context
    assert f"durable type=bead.reopened resource=bead/{project}-closed" in context
    assert "control type=event.gap resource=(none)" in context
    assert '"synthetic completed"' in context
    assert '"after":{"metadata":{},"status":"open"}' in context
    other = next(name for name in PROJECTS if name != project)
    assert f"memory:{other}" not in context and f"# Plug context ({other})" not in context
    return context


def _assert_catch_up(proc: subprocess.CompletedProcess[str], project: str) -> None:
    assert proc.returncode == 0, f"catch-me-up/{project} failed: {proc.stderr}"
    assert proc.stderr == "", f"unexpected catch-up stderr: {proc.stderr!r}"
    items = json.loads(proc.stdout)
    expected = [
        {
            "id": f"{project}-nikhil",
            "issue_type": "work_item",
            "status": "open",
            "title": f"title {project}-nikhil",
            "description": f"description {project}-nikhil",
            "priority": 1,
            "owner": None,
            "assignee": "user:nikhil",
            "created_at": "2026-11-05T00:00:01Z",
            "labels": [f"label:{project}"],
            "metadata": {"selected": True, "source": "synthetic"},
        },
        {
            "id": f"{project}-decision",
            "issue_type": "decision",
            "status": "blocked",
            "title": f"title {project}-decision",
            "description": None,
            "priority": 1,
            "assignee": "nikhil",
            "created_at": "2026-11-05T00:00:04Z",
            "updated_at": "2026-11-05T01:00:04Z",
            "labels": [f"label:{project}"],
            "metadata": {"selected": True, "source": "synthetic"},
        },
        {
            "id": f"{project}-null-decision",
            "issue_type": "decision",
            "status": "blocked",
            "title": f"title {project}-null-decision",
            "description": None,
            "priority": 1,
            "assignee": None,
            "created_at": "2026-11-05T00:00:05Z",
            "updated_at": "2026-11-05T01:00:05Z",
            "labels": [f"label:{project}"],
            "metadata": {"selected": True, "source": "synthetic"},
        },
    ]
    assert items[:3] == expected, json.dumps({"expected": expected, "actual": items}, sort_keys=True)
    assert items[3]["id"] == f"{project}-writable" and items[3]["assignee"] == "nikhil" and items[3]["status"] == "todo"
    assert "updated_at" not in items[3]
    if project == "cody-ops":
        assert len(items) == 5 and items[4]["id"] == f"{project}-reopened" and items[4]["status"] == "queued"
        assert items[4]["assignee"] is None
    else:
        assert len(items) == 4
    assert all("close_reason" not in row and "closed_at" not in row for row in items)
    assert all("fields" not in item and "project" not in item and "revision" not in item for item in items)
    assert "owner" in items[0] and items[0]["owner"] is None
    assert "owner" not in items[1] and "owner" not in items[2]
    assert items[2]["assignee"] is None


def main() -> int:
    server = _Server()
    thread = threading.Thread(target=server.serve_forever, name="plug-i8-fixture", daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="plug-i8-hook-fixture-") as directory:
            root = Path(directory)
            workspaces = {project: root / project for project in PROJECTS}
            for workspace in workspaces.values():
                workspace.mkdir()
            binding = root / "host-binding.json"
            binding.write_text(
                json.dumps(
                    {
                        "schema": "plug-host-binding/1",
                        "endpoint": f"http://127.0.0.1:{server.server_port}",
                        "incarnation": INCARNATION,
                        "principal": PRINCIPAL,
                        "agent": AGENT,
                        "projects": {
                            project: {"workspace": str(workspaces[project]), "legacy_ids": {}}
                            for project in PROJECTS
                        },
                    },
                    sort_keys=True,
                )
                + "\n"
            )

            hook_runs = 0
            canonical_payloads = 0
            for project in PROJECTS:
                claude_payload = _canonical_payload("claude", "SessionStart", project)
                claude = _run(HOOK, binding, project, "--provider", "claude", "--event", "SessionStart", stdin=claude_payload)
                baseline_context = _assert_hook(claude, "SessionStart", project)
                hook_runs += 1
                canonical_payloads += 1
                ignored_payload = dict(claude_payload)
                ignored_payload.update({"cwd": "/tmp/synthetic/ignored", "transcript_path": "/tmp/synthetic/ignored.jsonl", "provider_extension": {"ignored": True}})
                ignored = _run(HOOK, binding, project, "--provider", "claude", "--event", "SessionStart", stdin=ignored_payload)
                assert _assert_hook(ignored, "SessionStart", project) == baseline_context
                hook_runs += 1
                for event in EVENTS:
                    payload = _canonical_payload("codex", event, project)
                    codex = _run(HOOK, binding, project, "--provider", "codex", "--event", event, stdin=payload)
                    _assert_hook(codex, event, project)
                    hook_runs += 1
                    canonical_payloads += 1
                bad = _run(HOOK, binding, project, "--provider", "codex", "--event", "PreCompact", stdin={"hookEventName": "UserPromptSubmit", "cwd": "/tmp/synthetic"})
                assert bad.returncode == 1
                assert bad.stdout == ""
                assert "hook event input does not match configured event" in bad.stderr
                malformed = _run(HOOK, binding, project, "--provider", "codex", "--event", "PreCompact", raw_stdin="{")
                assert malformed.returncode == 1 and malformed.stdout == ""
                assert "hook input is not JSON" in malformed.stderr

            server.state.inject_stale_brief = True
            stale = _run(
                HOOK,
                binding,
                "cody-ops",
                "--provider",
                "claude",
                "--event",
                "SessionStart",
                stdin={"hook_event_name": "SessionStart", "cwd": "/tmp/synthetic"},
            )
            assert stale.returncode == 0
            assert "must be omitted after reopen" not in stale.stdout
            server.state.inject_stale_brief = False

            catchups = 0
            for project in PROJECTS:
                _assert_catch_up(_run(CATCH_UP, binding, project), project)
                catchups += 1

            server.state.unknown_status = True
            unknown = _run(CATCH_UP, binding, "cody-ops")
            assert unknown.returncode == 1 and unknown.stdout == "" and "ontology_status_unknown" in unknown.stderr
            server.state.unknown_status = False

            server.state.unknown_category = True
            unknown_category = _run(CATCH_UP, binding, "cody-ops")
            assert unknown_category.returncode == 1 and unknown_category.stdout == ""
            assert "ontology_category_unknown" in unknown_category.stderr
            server.state.unknown_category = False

            server.state.ontology_drift = True
            drift = _run(HOOK, binding, "cody-ops", "--provider", "claude", "--event", "SessionStart")
            assert drift.returncode == 1 and drift.stdout == "" and "ontology_changed_during_read" in drift.stderr
            server.state.ontology_drift = False

            with server.state.lock:
                requests = list(server.state.requests)
                failures = list(server.state.failures)
                auth_whoami_calls = server.state.auth_whoami_calls
                wire_rows = [row for project_rows in server.state.rows.values() for row in project_rows.values()]
            assert not failures, "; ".join(failures)
            assert auth_whoami_calls == 0, f"catalog-only auth.whoami was called {auth_whoami_calls} time(s)"
            assert all("assignee" not in row["bead"] and "updatedAt" not in row["bead"] for row in wire_rows)
            assert any("lease" in row for row in wire_rows)
            assert any("beads.source_fields" in row["bead"]["metadata"] for row in wire_rows)
            assert {request["params"].get("_meta", {}).get("project") for request in requests if request["method"] != "server.hello"} == set(PROJECTS)
            assert all(
                request["params"].get("_meta", {}).get("agent") == AGENT
                for request in requests
                if request["method"] != "server.hello"
            )
            hellos = [request for request in requests if request["method"] == "server.hello"]
            assert hellos and all("_meta" not in request["params"] for request in hellos)
            views = [request for request in requests if request["method"] == "view.run"]
            gets = [request for request in requests if request["method"] == "bead.get"]
            assert len(views) == (catchups + 1) * 4
            assert len(gets) == catchups * 7 + 6
            assert all(request["params"]["definition"]["filter"] == {} for request in views)
            assert all(request["params"]["definition"]["fields"] == FIELDS for request in views)
            assert all(request["params"]["definition"]["sort"] == ["revision"] for request in views)
            summary = {
                "status": "PASS",
                "fixture": "synthetic-local-http",
                "projects": list(PROJECTS),
                "hook_commands": 5,
                "hook_runs": hook_runs,
                "canonical_event_payloads_checked": canonical_payloads,
                "hook_input_policy": "event-name validated; provider payload fields intentionally ignored",
                "catch_up_runs": catchups,
                "view_pages": len(views),
                "bead_get_reads": len(gets),
                "rpc_requests": len(requests),
                "auth_whoami_calls": auth_whoami_calls,
                "hello_metadata_forbidden": True,
                "http_admission_headers_checked": True,
                "project_agent_metadata_checked": True,
                "principal_source": "agent.brief.result.whoami.principal",
                "legacy_record_comparison": True,
                "bead_get_wire_shape_checked": "separate lease.holder plus retained source fields",
                "category_selection": "installed per-project categories; same queued name active/done; native todo unchanged",
                "current_assignment_precedence": "writable presence including null, lease, workflow, retained source",
                "unknown_status_refusal": True,
                "unknown_category_refusal": True,
                "observed_ontology_drift_refusal": True,
                "ontology_content_atomic_snapshot": False,
            }
            print(json.dumps(summary, sort_keys=True))
            return 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
