"""Scoped legacy JSONL projection for the prepared milestone view."""
import json
import os
from pathlib import Path
import tempfile
import time
from plug_host import AdapterError, Client, RPCError


class Projection:
    def __init__(self, client):
        self.c = client
        self.path = client.workspace / (".plug-view-" + client.identity + ".jsonl")
        self.stale = False

    def fresh(self):
        try:
            if time.time() - self.path.stat().st_mtime < 15:
                return str(self.path)
        except FileNotFoundError:
            pass
        try:
            rows = [self.row(b) for b in self.c.snapshot()]
            fd, tmp = tempfile.mkstemp(prefix=".plug-view-", dir=self.path.parent)
            try:
                with os.fdopen(fd, "w") as out:
                    for row in rows:
                        out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(tmp, self.path)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            self.stale = False
            return str(self.path)
        except (AdapterError, OSError):
            if self.path.exists():
                self.stale = True
                return str(self.path)
            raise AdapterError("no validated projection available for this instance/project") from None

    def row(self, bead):
        if bead.get("project") != self.c.project:
            raise AdapterError("foreign project in projection")
        fields = bead.get("fields", {})
        status = bead["status"]
        if status not in {"open", "in_progress", "closed", "blocked", "deferred", "pinned", "frozen"}:
            raise AdapterError("unmapped imported status in projection")
        row = {"id": self.c.original(bead["id"]), "issue_type": bead["type"], "status": status, **fields}
        row["labels"] = bead.get("labels", fields.get("labels", []))
        row["close_reason"] = fields.get("close_reason", fields.get("result"))
        dependencies = []
        for link in bead.get("links", []):
            if link.get("from") != bead["id"]:
                continue
            target = link["to"]
            if link.get("targetKind") == "external":
                raise AdapterError("external graph targets need an explicit UI projection")
            if not Client.valid_ref(target):
                raise AdapterError("unsupported link target")
            dependencies.append({"depends_on_id": self.c.original(target["id"]), "type": link["kind"]})
        parent = bead.get("parent")
        if parent:
            if not isinstance(parent, str):
                raise AdapterError("hidden parent cannot be projected as legacy graph")
            rel = {"depends_on_id": self.c.original(parent), "type": "parent-child"}
            if rel not in dependencies:
                dependencies.append(rel)
        row["dependencies"] = dependencies
        return row
