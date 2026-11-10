#!/usr/bin/env python3
"""Public synthetic data; actual Plug transport and unmodified caller processes."""
import hashlib
import io
import json
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile

PROJECTS = ("cody-ops", "signals-v2")


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def build(source, destination):
    with tarfile.open(source) as archive:
        members = {item.name: archive.extractfile(item).read() for item in archive}
    manifest = json.loads(members["manifest.json"])
    records = {}
    for kind in manifest["kind_registry"]:
        records[kind["name"]] = [json.loads(line) for line in members[kind["file"]].splitlines()]
    combined = {kind: {} for kind in records}
    for project in PROJECTS:
        for kind, rows in records.items():
            for row in rows:
                # Relabel only public fixture identities before validation/import.
                row = json.loads(json.dumps(row).replace("demo", project))
                if row["source"]["table"] == "issue_versions":
                    continue
                if kind in ("edge", "comment"):
                    row["id"] = project + "-" + row["id"]
                    row["source"]["fields"]["id"] = row["id"]
                    if kind == "comment":
                        row["body"]["id"] = row["id"]
                if kind == "edge":
                    row["body"]["type"] = row["source"]["fields"]["type"] = "blocks"
                if kind == "bead":
                    fields = row["source"]["fields"]
                    suffix = row["id"][len(project):]
                    status, issue_type, assignee = {
                        "-7": ("open", "task", "legacy-owner"),
                        "-7.4": ("in_progress", "decision", None),
                        "-8": ("closed", "decision", None),
                    }[suffix]
                    fields.update(status=status, issue_type=issue_type, assignee=assignee,
                                  closed_at="2026-01-03T00:00:00Z" if status == "closed" else None,
                                  close_reason="old imported reason" if status == "closed" else None)
                    row["body"].update(source_type=issue_type, assignee=assignee, closed_at=None,
                                       status_category={"open": "unstarted", "in_progress": "started", "closed": "completed"}[status])
                    row["body"].pop("result", None)
                    if status == "closed":
                        row["body"].update(closed_at=fields["closed_at"], result="success")
                if kind == "beads-archive":
                    row["id"] = row["source"]["table"] + "/" + hashlib.sha256(encoded(row["source"]["fields"])).hexdigest()
                combined[kind][(row["project"], row["id"])] = row
    manifest["projects"] = [{"prefix": p, "id_mode": "hash", "hash_length": 1, "last_id": 8,
                             "child_counters": {p + "-7": 4}} for p in PROJECTS]
    manifest["history"] = "unavailable"
    manifest["ontology"] = [{"id": p + "-decision", "kind": "map_type", "target": {"project": p, "type": "decision"},
                              "from": "decision", "to": "issue"} for p in PROJECTS]
    manifest["verification"]["ontology"] = "built-in status mapping; explicit map_type directives recorded in manifest"
    counts = {}
    for kind, keyed in combined.items():
        rows = [keyed[key] for key in sorted(keyed)]
        members["records/" + kind + ".ndjson"] = b"".join(encoded(row) for row in rows)
        for row in rows:
            table = row["source"]["table"]
            counts[table] = counts.get(table, 0) + 1
            if kind == "bead":
                counts["labels"] = counts.get("labels", 0) + len(row["body"]["labels"])
    for source in manifest["sources"]:
        family = source["family"]
        if family in counts or family == "issue_versions":
            source["rows"] = counts.get(family, 0)
        if family in ("issues.row_lock", "issues.is_blocked"):
            source["rows"] = counts["issues"]
    for kind in manifest["kind_registry"]:
        kind["count"] = len(combined[kind["name"]])
    for name in manifest["files"]:
        manifest["files"][name] = {"sha256": hashlib.sha256(members[name]).hexdigest(), "size": len(members[name])}
    members["manifest.json"] = encoded(manifest)
    with tarfile.open(destination, "w", format=tarfile.USTAR_FORMAT) as archive:
        for name in ["manifest.json"] + sorted(name for name in members if name != "manifest.json"):
            header = tarfile.TarInfo(name)
            header.mode, header.size = 0o600, len(members[name])
            archive.addfile(header, io.BytesIO(members[name]))


def run(binding_path, native_ids, phase):
    from render import render
    from render_fixture import DISPOSABLE, public_snapshot
    DISPOSABLE.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="i8-actual-render-", dir=DISPOSABLE) as directory:
        root = Path(directory)
        callers = root / "callers ' \" $(touch actual-injected-dollar) `touch actual-injected-tick`; name"
        callers.mkdir()
        source = Path(__file__).resolve().parent
        for name in ("binding.py", "hook_adapter.py", "catch_me_up_adapter.py"):
            shutil.copyfile(source / name, callers / name)
        bound = root / "binding ' \" $(touch actual-injected-binding); name.json"
        shutil.copyfile(binding_path, bound)
        snapshot = public_snapshot(root)
        prepared = root / "rendered"
        render(snapshot, prepared, callers, bound)
        result = run_rendered(str(bound), native_ids, prepared, root, phase)
        assert not any((root / name).exists() for name in ("actual-injected-dollar", "actual-injected-tick", "actual-injected-binding"))
        return result


def run_rendered(binding_path, native_ids, prepared, cwd, phase):
    from binding import Client
    from catch_me_up_adapter import _legacy_record
    from hook_adapter import _typed_close_reason
    from render_fixture import hook_commands, shell_run, skill_commands
    assert phase in ("closed", "reopened")
    result = {"schema": "plug-i8-actual-boundary/1", "instance_count": 1, "projects": {},
              "command_source": "rendered settings and full skill", "shell_metacharacter_paths": True,
              "lifecycle_phase": phase}
    catches = skill_commands(prepared)
    failures = []
    for project in PROJECTS:
        client = Client(binding_path, project)
        brief = client.brief(2048)
        wire = {}
        created = native_ids[project]
        for ident in (project + "-7", project + "-7.4", project + "-8", *created.values()):
            wire[ident] = client.rpc("bead.get", {"id": {"id": ident}})
        assert wire[project + "-7"]["bead"]["metadata"]["beads.source_fields"]["assignee"] == "legacy-owner"
        assert wire[project + "-7"]["bead"]["fields"]["assignee"] == "nikhil"
        assert wire[project + "-7.4"]["bead"]["metadata"]["beads.source_fields"]["issue_type"] == "decision"
        assert wire[project + "-7.4"]["bead"]["type"] == "issue"
        assert wire[created["claimed"]]["lease"]["holder"] == "user:nikhil"
        assert all("assignee" not in item["bead"] and "updatedAt" not in item["bead"] for item in wire.values())
        ontology = client.ontology()
        assert ontology.category("shared") == ("active" if project == "cody-ops" else "done")
        native_categories = {name: ontology.category(name) for name in ("todo", "in_progress", "done", "backlog", "shared", "caller-active", "caller-done")}
        assert native_categories == {"todo": "active", "in_progress": "wip", "done": "done", "backlog": "frozen",
                                     "shared": "active" if project == "cody-ops" else "done",
                                     "caller-active": "active", "caller-done": "done"}
        assert wire[created["ready"]]["bead"]["status"] == "caller-active"
        assert wire[created["claimed"]]["bead"]["status"] == "in_progress"
        assert wire[created["frozen"]]["bead"]["status"] == "backlog"
        assert not wire[created["frozen"]].get("lease")
        ready = {item["id"] for item in brief["readyTop"]}
        assert created["ready"] in ready
        assert created["claimed"] not in ready and created["frozen"] not in ready
        assert wire[project + "-8"]["bead"]["metadata"]["beads.source_fields"]["close_reason"] == "old imported reason"
        assert wire[project + "-8"]["bead"]["metadata"]["i8.host.close_reason"]["reason"] == "new typed reason"
        lifecycle = wire[project + "-8"]["bead"]
        selected_status = "caller-done" if phase == "closed" else "caller-active"
        assert lifecycle["status"] == selected_status
        assert ontology.terminal(selected_status) == (phase == "closed")
        # Exercise the same projection functions on the actual bead.get wire
        # shape as well as the emitted commands, which omit terminal rows.
        projected = _legacy_record(lifecycle, project, ontology)
        assert projected["status"] == selected_status
        if phase == "closed":
            assert projected["close_reason"] == "new typed reason"
            assert _typed_close_reason(lifecycle, ontology) == "new typed reason"
        else:
            assert "close_reason" not in projected and "closed_at" not in projected
            assert _typed_close_reason(lifecycle, ontology) is None
        hook_count = 0
        for provider, event, emitted in hook_commands(prepared, project):
            payload = {"hook_event_name": event, "session_id": "synthetic", "cwd": "/synthetic/ignored",
                       "transcript_path": "/synthetic/ignored.jsonl", "source": "resume"}
            completed = shell_run(emitted, cwd, json.dumps(payload))
            assert completed.returncode == 0, completed.stderr
            output = json.loads(completed.stdout)
            assert set(output) == {"hookSpecificOutput"}
            envelope = output["hookSpecificOutput"]
            assert set(envelope) == {"hookEventName", "additionalContext"} and envelope["hookEventName"] == event
            assert "# Plug context (" + project + ")" in envelope["additionalContext"]
            other = next(p for p in PROJECTS if p != project)
            assert "Synthetic " + other not in envelope["additionalContext"]
            assert "A complete synthetic memory" in envelope["additionalContext"]
            ready_context = envelope["additionalContext"].split("\n## Ready\n", 1)[1].split("\n## Recent changes\n", 1)[0]
            assert created["ready"] in ready_context
            assert created["claimed"] not in ready_context and created["frozen"] not in ready_context
            hook_count += 1
        assert hook_count == 5
        completed = shell_run(catches[project], cwd)
        expected = set(wire)
        if phase == "closed":
            expected.remove(project + "-8")
        if project == "signals-v2":
            expected.remove(project + "-7.4")
        actual = json.loads(completed.stdout) if completed.returncode == 0 else []
        ids = {row["id"] for row in actual}
        if completed.returncode or ids != expected:
            failures.append({"project": project, "exit": completed.returncode, "stderr": completed.stderr,
                             "missing": sorted(expected - ids), "extra": sorted(ids - expected)})
        for row in actual:
            if row["id"] not in wire:
                continue
            source = wire[row["id"]]["bead"].get("metadata", {}).get("beads.source_fields")
            if source is not None:
                for key in ("issue_type", "updated_at", "created_at", "metadata"):
                    assert row.get(key) == source[key] and key in row, (row["id"], key)
                assert row["status"] == wire[row["id"]]["bead"]["status"]
                assert row["assignee"] == ("nikhil" if row["id"].endswith("-7") else source["assignee"])
                assert not any(key.startswith(("beads.", "plug.restore.")) for key in (row.get("metadata") or {}))
                assert "closed_at" not in row and "close_reason" not in row
            elif row["id"] == created["claimed"]:
                assert row["assignee"] == "user:nikhil" and "updated_at" not in row
            else:
                assert row["assignee"] == "nikhil" and "updated_at" not in row
                assert row["status"] == ("backlog" if row["id"] == created["frozen"] else "caller-active")
        result["projects"][project] = {"hook_commands": hook_count, "expected_rows": len(expected), "actual_rows": len(actual),
                                       "wire_statuses": sorted({item["bead"]["status"] for item in wire.values()}),
                                       "separate_lease_holder": wire[created["claimed"]]["lease"]["holder"],
                                       "source_metadata_readback": True, "installed_native_categories": native_categories,
                                       "unleased_frozen_not_ready": True, "active_ready_in_hook": True,
                                       "native_status_names_unchanged": True, "selected_lifecycle_status": selected_status,
                                       "typed_resolution_projected": phase == "closed",
                                       "stale_resolution_suppressed": phase == "reopened"}
    result.update(status="FAIL" if failures else "PASS", failures=failures)
    print(json.dumps(result, sort_keys=True))
    return bool(failures)


if __name__ == "__main__":
    if sys.argv[1] == "build":
        build(*sys.argv[2:])
    else:
        raise SystemExit(run(sys.argv[2], json.loads(sys.argv[3]), sys.argv[4]))
