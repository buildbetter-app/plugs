"""Small, explicit Plug caller transport used by the prepared host adapters.

The adapter deliberately accepts one absolute binding file.  Project names,
workspaces, the endpoint and the authenticated principal are all checked from
that file; no caller may infer a project from cwd, labels or environment.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
import uuid


class AdapterError(RuntimeError):
    """A typed, caller-safe preparation or transport failure."""


class RPCError(AdapterError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(f"Plug RPC refused: {code}")


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AdapterError("binding is not readable JSON") from exc


def _same_principal(left, right) -> bool:
    return isinstance(left, dict) and isinstance(right, dict) and left == right


class Client:
    """A credential-free-origin JSON-RPC caller with one project binding."""

    PROJECTS = {"cody-ops", "signals-v2"}

    def __init__(self, binding: str, project: str):
        binding_path = Path(binding)
        if not binding_path.is_absolute():
            raise AdapterError("--binding must be an absolute path")
        config = _read_json(binding_path)
        if config.get("schema") != "plug-host-binding/1":
            raise AdapterError("binding schema must be plug-host-binding/1")
        projects = config.get("projects")
        if not isinstance(projects, dict) or set(projects) != self.PROJECTS:
            raise AdapterError("binding must name exactly cody-ops and signals-v2")
        if project not in self.PROJECTS:
            raise AdapterError("--project must be cody-ops or signals-v2")
        endpoint = config.get("endpoint")
        if not isinstance(endpoint, str):
            raise AdapterError("binding endpoint is required")
        parsed = urllib.parse.urlsplit(endpoint or "")
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise AdapterError("binding endpoint must be a credential-free HTTP(S) origin")
        if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise AdapterError("non-loopback binding endpoint must use HTTPS")
        incarnation = config.get("incarnation")
        principal = config.get("principal")
        agent = config.get("agent")
        if not isinstance(incarnation, str) or not incarnation:
            raise AdapterError("binding incarnation is required")
        if not isinstance(principal, dict) or not principal:
            raise AdapterError("binding principal is required")
        if not isinstance(agent, str) or not agent:
            raise AdapterError("binding agent is required")
        token_env = config.get("token_env")
        if token_env is not None and (not isinstance(token_env, str) or not token_env):
            raise AdapterError("binding token_env must be a nonempty string when present")

        for name, entry in projects.items():
            if not isinstance(entry, dict):
                raise AdapterError(f"project {name} binding is not an object")
            workspace = entry.get("workspace")
            if not isinstance(workspace, str) or not Path(workspace).is_absolute():
                raise AdapterError(f"project {name} workspace must be absolute")
            ids = entry.get("legacy_ids")
            if not isinstance(ids, dict):
                raise AdapterError(f"project {name} legacy_ids must be an object")
            for original, ref in ids.items():
                if not isinstance(original, str) or not original:
                    raise AdapterError("legacy IDs must remain nonempty strings")
                if not isinstance(ref, dict) or set(ref) != {"id"} or not isinstance(ref["id"], str) or not ref["id"]:
                    raise AdapterError("legacy IDs must map to canonical {id: string} refs")
        for name, entry in projects.items():
            refs = [ref["id"] for ref in entry["legacy_ids"].values()]
            if len(set(refs)) != len(refs):
                raise AdapterError(f"project {name} legacy_ids must be one-to-one")

        self.binding = config
        self.project = project
        self.endpoint = endpoint.rstrip("/") + "/rpc/v1"
        self.incarnation = incarnation
        self.principal = principal
        self.agent = agent
        workspaces = [Path(entry["workspace"]).resolve() for entry in projects.values()]
        if len(set(workspaces)) != len(workspaces):
            raise AdapterError("project workspaces must be distinct")
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

        hello = self.rpc("server.hello", {})
        if hello.get("incarnation") != incarnation:
            raise AdapterError("server incarnation differs from binding")
    def brief(self, budget_tokens: int = 1) -> dict:
        """Read the bound context and validate the actual composed principal.

        ``auth.whoami`` is advertised by the catalog but is not mounted by the
        inspected server composition.  ``agent.brief`` is the available typed
        identity-bearing read, so every prepared caller uses its
        ``result.whoami.principal`` before consuming project data.
        """
        result = self.rpc("agent.brief", {"project": self.project, "budgetTokens": budget_tokens})
        whoami = result.get("whoami")
        if not isinstance(whoami, dict) or not _same_principal(whoami.get("principal"), self.principal):
            raise AdapterError("agent.brief principal differs from binding")
        return result

    def ontology(self) -> "ProjectOntology":
        return ProjectOntology(self)

    def rpc(self, method: str, params: dict) -> dict:
        if not isinstance(params, dict) or "_meta" in params:
            raise AdapterError("caller parameters must be an object without _meta")
        request_id = str(uuid.uuid4())
        payload = copy.deepcopy(params)
        # The pinned composition accepts the metadata-free handshake only.
        # Every subsequent project RPC carries both explicit routing fields.
        if method != "server.hello":
            payload["_meta"] = {"project": self.project, "agent": self.agent}
        request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": payload}
        # Match the actual HTTP transport's non-browser admission contract.
        headers = {"Plug-RPC": "1", "Content-Type": "application/json", "Accept": "application/json"}
        token_env = self.binding.get("token_env")
        if token_env:
            token = os.environ.get(token_env)
            if not token:
                raise AdapterError("binding token environment is unavailable")
            headers["Authorization"] = "Bearer " + token
        try:
            req = urllib.request.Request(self.endpoint, data=json.dumps(request, ensure_ascii=False).encode(), headers=headers)
            with self._opener.open(req, timeout=120) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise AdapterError("RPC response exceeds the caller bound")
            envelope = json.loads(raw)
        except (OSError, ValueError, urllib.error.URLError) as exc:
            raise AdapterError("RPC transport/result unavailable") from exc
        if not isinstance(envelope, dict) or envelope.get("jsonrpc") != "2.0" or envelope.get("id") != request_id:
            raise AdapterError("RPC response identity does not match request")
        has_result = "result" in envelope
        has_error = "error" in envelope
        if has_result == has_error:
            raise AdapterError("RPC response must contain exactly one result or error")
        if has_error:
            error = envelope.get("error")
            if not isinstance(error, dict):
                raise AdapterError("RPC error envelope is malformed")
            data = error.get("data") if isinstance(error, dict) else None
            code = data.get("code") if isinstance(data, dict) else None
            raise RPCError(code or "unknown_error")
        result = envelope.get("result")
        if not isinstance(result, dict):
            raise AdapterError("RPC result must be an object")
        return result


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise AdapterError("RPC redirects are refused")


class ProjectOntology:
    """Read the installed project-effective ontology; never infer status names.

    The first six categories are Plug's pinned public vocabulary. The four
    Beads categories are the native import contract, consumed without renaming
    or category inversion when supplied by the installed ontology successor.
    A server lacking this typed read remains a dependency refusal.
    """

    ACTIVE = {"triage", "backlog", "unstarted", "started", "active", "wip", "frozen"}
    TERMINAL = {"completed", "canceled", "done"}

    def __init__(self, client: Client):
        self.client = client
        self.result = client.rpc("ontology.get", {"project": client.project})
        document = self.result.get("document")
        if (self.result.get("drifted") is not False
                or self.result.get("problems") not in (None, [])
                or not isinstance(document, dict)
                or not isinstance(document.get("statuses"), dict)
                or not document["statuses"]
                or "revision" not in self.result):
            raise RPCError("ontology_unavailable")
        self.statuses = document["statuses"]
        for name, status in self.statuses.items():
            if (not isinstance(name, str) or not name or not isinstance(status, dict)
                    or not isinstance(status.get("name"), str) or not status["name"]
                    or status.get("category") not in self.ACTIVE | self.TERMINAL):
                raise RPCError("ontology_category_unknown")

    def category(self, status: str) -> str:
        if not isinstance(status, str) or status not in self.statuses:
            raise RPCError("ontology_status_unknown")
        return self.statuses[status]["category"]

    def terminal(self, status: str) -> bool:
        return self.category(status) in self.TERMINAL

    def check_unchanged(self) -> None:
        # Content reads share one asOf. The protected current ontology API does
        # not accept asOf, so refuse a changed revision/document before output.
        if self.client.rpc("ontology.get", {"project": self.client.project}) != self.result:
            raise RPCError("ontology_changed_during_read")
