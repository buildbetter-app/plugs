"""Typed, explicitly bound Plug host adapter. No Beads subprocess or discovery."""
import contextlib
import copy
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class AdapterError(RuntimeError):
    pass


class TransportError(AdapterError):
    """Only a network outage permits explicitly marked stale projection fallback."""
    pass


class RPCError(AdapterError):
    def __init__(self, code):
        self.code = code
        super().__init__(f"Plug RPC refused: {code}")


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def read_json(path, default=None):
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else copy.deepcopy(default)


def atomic_json(path, value):
    path = Path(path)
    fd, tmp = tempfile.mkstemp(prefix=".plug-host-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(encoded(value) + b"\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        dfd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Client:
    def __init__(self, binding_path, project, workspace=None):
        binding_path = Path(binding_path)
        if not binding_path.is_absolute():
            raise AdapterError("binding must be an absolute path")
        self.binding = read_json(binding_path)
        cfg = self.binding
        if cfg.get("schema") != "plug-host-binding/1" or set(cfg.get("projects", {})) != {"cody-ops", "signals-v2"}:
            raise AdapterError("binding requires exactly cody-ops and signals-v2 in one instance")
        self.project = project
        p = cfg["projects"][project]
        self.workspace = Path(p["workspace"])
        if not self.workspace.is_absolute() or not self.workspace.is_dir():
            raise AdapterError("bound workspace must be an existing absolute directory")
        self.workspace = self.workspace.resolve()
        if workspace is not None and Path(workspace).resolve() != self.workspace:
            raise AdapterError("workspace disagrees with explicit project binding")
        workspaces = [str(Path(x["workspace"]).resolve()) for x in cfg["projects"].values()]
        if len(set(workspaces)) != 2:
            raise AdapterError("project sidecar workspaces must be distinct")
        self.ids = p["legacy_ids"]
        if not isinstance(self.ids, dict):
            raise AdapterError("legacy_ids must be an explicit per-project object")
        for old, ref in self.ids.items():
            if not isinstance(old, str) or not old or not self.valid_ref(ref):
                raise AdapterError("invalid lossless imported ID map")
        if len({x["id"] for x in self.ids.values()}) != len(self.ids):
            raise AdapterError("legacy ID map must be one-to-one within each project")
        self.reverse_ids = {v["id"]: k for k, v in self.ids.items()}
        url = urllib.parse.urlsplit(cfg["endpoint"])
        if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in {"", "/"}:
            raise AdapterError("endpoint must be a credential-free HTTP(S) origin")
        if url.scheme == "http" and url.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise AdapterError("non-loopback endpoint requires HTTPS")
        self.endpoint = cfg["endpoint"].rstrip("/") + "/rpc/v1"
        self.identity = digest(cfg)
        self.scope = digest({"endpoint": cfg["endpoint"], "incarnation": cfg["incarnation"],
                             "principal": cfg["principal"], "project": project})
        self.generated = set()
        ledger = read_json(self.workspace / ".plug-host-journal.json", {"jobs": {}})
        for job in ledger.get("jobs", {}).values():
            if job.get("scope") != self.scope:
                continue
            for call in job.get("calls", []):
                if "result" not in call:
                    continue
                if call["method"] == "bead.create":
                    self.generated.add(created_ref(call["result"]))
                elif call["method"] == "tx.apply":
                    ops = call["attempts"][-1]["params"]["changeSet"]["ops"]
                    for i, op in enumerate(ops):
                        if op["kind"] == "create":
                            self.generated.add(created_ref(call["result"], i))
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self.verify_identity()

    def verify_identity(self):
        cfg = self.binding
        hello = self.rpc("server.hello", {})
        if hello.get("incarnation") != cfg["incarnation"]:
            raise AdapterError("server incarnation differs from binding")
        who = self.rpc("agent.brief", {})["whoami"]
        if who.get("principal") != cfg["principal"]:
            raise AdapterError("transport principal differs from binding")

    @staticmethod
    def valid_ref(ref):
        return isinstance(ref, dict) and set(ref) == {"id"} and isinstance(ref["id"], str) and bool(ref["id"])

    def rpc(self, method, params, key=None):
        params = copy.deepcopy(params)
        if method != "server.hello":
            params["_meta"] = {"project": self.project, "agent": self.binding["agent"]}
        if key:
            params["_meta"]["idempotencyKey"] = key
        request_id = str(uuid.uuid4())
        request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        headers = {"Content-Type": "application/json"}
        token_env = self.binding.get("token_env")
        if token_env:
            token = os.environ.get(token_env)
            if not token:
                raise AdapterError("bound credential environment variable is unavailable")
            headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(self.endpoint, data=encoded(request), headers=headers)
        try:
            with self._opener.open(req, timeout=120) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise AdapterError("RPC response exceeds bound")
            result = json.loads(raw)
        except urllib.error.HTTPError as error:
            # HTTP refusals are authority/result failures, not an offline cache grant.
            codes = {401: "unauthenticated", 403: "forbidden", 404: "not_found", 429: "rate_limited"}
            raise RPCError(codes.get(error.code, "http_refused")) from None
        except (OSError, urllib.error.URLError):
            raise TransportError("RPC transport unavailable; retain journal for exact replay") from None
        except ValueError:
            raise AdapterError("RPC result is malformed") from None
        if result.get("id") != request_id or result.get("jsonrpc") != "2.0":
            raise AdapterError("RPC response identity mismatch")
        if "error" in result:
            raise RPCError(result["error"].get("data", {}).get("code", "unknown_error"))
        if "result" not in result:
            raise AdapterError("RPC result is absent")
        return result["result"]

    def ref(self, original):
        if not isinstance(original, str) or not original:
            raise AdapterError("ID must be a nonempty exact string")
        if original in self.ids:
            return copy.deepcopy(self.ids[original])
        if original in self.generated:
            return {"id": original}
        raise AdapterError("ID has neither an explicit import mapping nor a scoped create receipt")

    def get(self, original, as_of=None):
        return self._get_ref(self.ref(original), as_of)

    def _get_ref(self, ref, as_of=None):
        params = {"id": ref}
        if as_of is not None:
            params["asOf"] = as_of
        bead = self.rpc("bead.get", params)["bead"]
        if bead.get("project") != self.project or bead.get("id") != params["id"]["id"]:
            raise AdapterError("reference resolved outside the bound project")
        return bead

    def original(self, canonical):
        return self.reverse_ids.get(canonical, canonical)

    def snapshot(self):
        # view.run supplies a pinned page chain; point reads use that exact asOf.
        params = {"definition": {"filter": {}, "fields": ["id"], "budgetTokens": 1000}}
        ids, seen, as_of = [], set(), None
        while True:
            page = self.rpc("view.run", params)
            if as_of is None:
                as_of = page["asOf"]
                if as_of.get("incarnation") != self.binding["incarnation"]:
                    raise AdapterError("snapshot incarnation differs from binding")
            elif page["asOf"] != as_of:
                raise AdapterError("projection pages changed snapshot")
            ids.extend(item["id"] for item in page["items"])
            cursor = page.get("nextCursor")
            if not cursor:
                break
            if cursor in seen or len(ids) > 20000:
                raise AdapterError("projection did not advance within bound")
            seen.add(cursor)
            params["cursor"] = cursor
        return [self._get_ref({"id": i}, as_of) for i in ids]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise AdapterError("RPC redirects are refused")


class Job:
    """Durable outbox with local recovery. Does not promise distributed atomicity."""
    def __init__(self, client, name, payload, defaults):
        self.client = client
        self.path = client.workspace / ".plug-host-journal.json"
        self.job_id = digest({"binding": client.identity, "name": name, "payload": payload})
        self.defaults = defaults
        self.index = 0

    def __enter__(self):
        try:
            return self._enter()
        except BaseException:
            if hasattr(self, "lock"):
                self.lock.close()
            raise

    def _enter(self):
        self.lock = open(self.client.workspace / ".plug-host.lock", "a+")
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        self.book = read_json(self.path, {"schema": "plug-host-journal/1", "jobs": {}, "active": None})
        if self.book.get("schema") != "plug-host-journal/1":
            raise AdapterError("unknown journal schema")
        active = self.book["active"]
        if active is not None and active != self.job_id:
            raise AdapterError("unfinished different job; recover its exact inputs first")
        jobs = self.book["jobs"]
        if self.job_id not in jobs:
            jobs[self.job_id] = {"initial": {k: read_json(self.client.workspace / k, v) for k, v in self.defaults.items()},
                                 "calls": [], "scope": self.client.scope, "stamp": time.strftime("%Y%m%d-%H%M%S"), "done": False}
        self.state = jobs[self.job_id]
        self.done = self.state["done"]
        self.data = copy.deepcopy(self.state["initial"])
        if not self.done:
            self.book["active"] = self.job_id
            self.save()
            if "outputs" in self.state:
                self.finish(self.state["outputs"])
                self.done = True
            else:
                self.check_inputs()
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()

    def save(self):
        atomic_json(self.path, self.book)

    def check_inputs(self):
        for name, initial in self.state["initial"].items():
            current = read_json(self.client.workspace / name, self.defaults[name])
            target = self.state.get("outputs", {}).get(name, initial)
            if current != initial and current != target:
                raise AdapterError("sidecar changed outside pending job; manual reconciliation required")

    def mutate(self, method, build):
        pos = self.index
        self.index += 1
        calls = self.state["calls"]
        if pos == len(calls):
            calls.append({"method": method, "attempts": []})
        call = calls[pos]
        if call["method"] != method:
            raise AdapterError("journal operation sequence changed")
        if "result" in call:
            return copy.deepcopy(call["result"])
        for _ in range(3):
            attempts = call["attempts"]
            if not attempts or "error" in attempts[-1]:
                params = build()
                attempts.append({"key": str(uuid.uuid4()), "params": params})
                self.save()  # exact request and random key precede any network write
            attempt = attempts[-1]
            try:
                result = self.client.rpc(method, attempt["params"], attempt["key"])
            except RPCError as e:
                attempt["error"] = e.code
                self.save()
                if e.code == "precondition_failed":
                    continue  # definite non-commit: reread and merge, with a new key
                raise
            call["result"] = result
            self.save()
            return copy.deepcopy(result)
        raise AdapterError("CAS contention persisted for three attempts; pending job retained")

    def finish(self, outputs):
        self.state["outputs"] = copy.deepcopy(outputs)
        self.save()
        self.check_inputs()
        for name, value in outputs.items():
            if name not in self.defaults or Path(name).name != name:
                raise AdapterError("unbound sidecar output")
            atomic_json(self.client.workspace / name, value)
        self.state["done"] = True
        self.book["active"] = None
        self.save()


def created_ref(commit, index=0):
    try:
        ref = commit["results"][index]["ref"]
    except (KeyError, IndexError, TypeError):
        raise AdapterError("create result has no positional Commit.results ref") from None
    if not Client.valid_ref(ref):
        raise AdapterError("create result is not an explicit main-branch ID ref")
    return ref["id"]


def expect(bead):
    return {"ref": {"id": bead["id"]}, "revision": bead["revision"]}


class Writes:
    def __init__(self, client, job):
        self.c, self.job = client, job

    def create(self, typ, fields, parent=None):
        def build():
            p = {"type": typ, "fields": fields}
            if parent is not None:
                self.c.get(parent)
                p["parent"] = self.c.ref(parent)
            return p
        ident = created_ref(self.job.mutate("bead.create", build))
        self.c.generated.add(ident)
        return ident

    def update(self, original, fields):
        return self.job.mutate("bead.update", lambda: {"id": self.c.ref(original), "fields": fields, "expect": [expect(self.c.get(original))]})

    def labels(self, originals, add=(), remove=()):
        if not originals:
            return
        def build():
            ops, conditions = [], []
            for original in originals:
                bead = self.c.get(original)
                old = bead.get("labels", bead.get("fields", {}).get("labels", [])) or []
                labels = [x for x in old if x not in remove]
                labels.extend(x for x in add if x not in labels)
                ops.append({"kind": "patch", "target": self.c.ref(original), "fields": {"labels": labels}})
                conditions.append(expect(bead))
            return {"changeSet": {"ops": ops, "expect": conditions}}
        return self.job.mutate("tx.apply", build)

    def link(self, dependent, prerequisite, remove=False):
        def build():
            source, target = self.c.get(dependent), self.c.get(prerequisite)
            return {"from": self.c.ref(dependent), "to": self.c.ref(prerequisite), "kind": "blocks", "expect": [expect(source), expect(target)]}
        return self.job.mutate("bead.unlink" if remove else "bead.link", build)

    def lifecycle(self, original, close=True, reason=""):
        if not close:
            return self.job.mutate("bead.reopen", lambda: {"id": self.c.ref(original), "reason": reason, "expect": [expect(self.c.get(original))]})
        def build():
            bead = self.c.get(original)
            target = self.c.ref(original)
            return {"changeSet": {"ops": [
                {"kind": "close", "target": target, "reason": reason},
                {"kind": "setMeta", "target": target, "key": "i8.host.close_reason",
                 "value": {"schema": "plug-host-close-reason/1", "reason": reason}},
            ], "expect": [expect(bead)]}}
        return self.job.mutate("tx.apply", build)


def fields_for(item, doc=None, project="signals-v2"):
    description = item.get("description", item.get("desc", ""))
    if doc is not None:
        description = f"{description}\n\nPlan key: {item['key']}. Design doc: {doc}"
    labels = [x for x in item.get("labels", []) if x != "decision"]
    if project == "signals-v2" and project not in labels:
        labels.append(project)
    fields = {"title": item["title"], "description": description,
              "priority": int(item.get("priority", item.get("prio", 2))), "labels": labels}
    for src, dst in [("design", "design"), ("acceptance", "acceptance_criteria"), ("acceptance_criteria", "acceptance_criteria"),
                     ("owner", "assignee"), ("ext", "external_ref")]:
        if item.get(src) is not None:
            fields[dst] = item[src]
    if item.get("due"):
        # No ambient clock/natural-language interpretation. Reject unsupported due strings.
        due = datetime.datetime.fromisoformat(item["due"].replace("Z", "+00:00"))
        if due.tzinfo is None:
            due = due.replace(tzinfo=datetime.timezone.utc)
        fields["due_at"] = due.isoformat().replace("+00:00", "Z")
    typ = "decision" if "decision" in item.get("labels", []) else item["type"]
    return typ, fields
