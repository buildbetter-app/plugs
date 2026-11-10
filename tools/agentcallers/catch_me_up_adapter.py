#!/usr/bin/env python3
"""Adapt the project-bound Plug reads to the legacy catch-me-up JSON array.

The public skill used ``bd list --status open,in_progress,blocked --json``
and then kept rows assigned to ``nikhil`` or whose type was ``decision``.
The installed project ontology determines active versus terminal categories.
The adapter pages one unfiltered view, reads full beads at that same snapshot,
applies assignment/decision selection, and preserves native status names in
the legacy JSON array. Unknown ontology statuses are typed refusals.
"""

from __future__ import annotations

import argparse
import json
import sys

from binding import AdapterError, Client


# These are the only fields admitted by the pinned ordinary view service that
# are needed to identify/filter rows. A top-level assignee or updatedAt is
# unsupported; assignment comes from fields and the separate bead.get lease.
FIELDS = ["id", "type", "project", "status", "fields", "metadata", "labels", "revision"]
CLOSE_REASON_KEY = "i8.host.close_reason"
CLOSE_REASON_SCHEMA = "plug-host-close-reason/1"

# Plug fields are the imported legacy ontology payload. Flatten every field
# into the old record, preserving present-null versus omitted semantics while
# keeping Plug envelope/control members out of the caller shape.
LEGACY_RESERVED_FIELDS = {"id", "type", "project", "revision", "fields", "metadata", "labels", "parent", "assignee", "status", "issue_type", "closed_at", "close_reason"}
CORE_TIME_FIELDS = (
    ("createdAt", "created_at"),
    ("updatedAt", "updated_at"),
    ("closedAt", "closed_at"),
)
SOURCE_FIELDS_KEY = "beads.source_fields"
SOURCE_FIELD_PREFIX = "beads.source."
RESERVED_METADATA_PREFIXES = ("beads.", "plug.restore.")


def _value(item: dict, key: str):
    direct = item.get(key)
    if direct is not None:
        return direct
    fields = item.get("fields")
    if isinstance(fields, dict):
        return fields.get(key)
    return None


def _validate_lifecycle_projection(item: dict, ontology) -> None:
    """Enforce the shared CloseOp+typed-metadata projection boundary."""
    metadata = item.get("metadata")
    typed = metadata.get(CLOSE_REASON_KEY) if isinstance(metadata, dict) else None
    status = _value(item, "status")
    if not ontology.terminal(status):
        # SetMeta persists across canonical reopen; suppress the old resolution
        # in projection, without requiring the separately owned writer to erase it.
        return
    if typed is not None and (
        not isinstance(typed, dict)
        or typed.get("schema") != CLOSE_REASON_SCHEMA
        or not isinstance(typed.get("reason"), str)
    ):
        raise AdapterError("closed view item has an invalid typed close reason")


def _copy_if_present(target: dict, source: dict, source_key: str, target_key: str | None = None) -> None:
    """Copy a field while preserving present-null versus omitted semantics."""
    if source_key in source:
        target[target_key or source_key] = source[source_key]


def _source_fields(bead: dict) -> dict | None:
    """Decode the retained legacy field map emitted by the restore importer."""
    metadata = bead.get("metadata")
    if not isinstance(metadata, dict) or SOURCE_FIELDS_KEY not in metadata:
        return None
    value = metadata[SOURCE_FIELDS_KEY]
    if not isinstance(value, dict):
        raise AdapterError("restored bead source fields are not an object")
    return value


def _source_value(bead: dict, source_fields: dict | None, key: str):
    """Read a retained source field while preserving explicit null values."""
    if isinstance(source_fields, dict) and key in source_fields:
        return True, source_fields[key]
    metadata = bead.get("metadata")
    marker = f"{SOURCE_FIELD_PREFIX}{key}"
    if isinstance(metadata, dict) and marker in metadata:
        return True, metadata[marker]
    return False, None


def _assignment(bead: dict, source_fields: dict | None) -> tuple[bool, object]:
    """Current writable field (including null), live lease, workflow, source.

    Presence matters: an explicit current null clears the legacy assignment;
    an absent current field allows a live canonical lease to supersede import
    provenance. Source strings/null remain the fallback for unchanged imports.
    """
    fields = bead.get("fields")
    if isinstance(fields, dict) and "assignee" in fields:
        return True, fields["assignee"]
    lease = bead.get("lease")
    if isinstance(lease, dict) and "holder" in lease:
        return True, lease["holder"]
    if "assignee" in bead:
        return True, bead["assignee"]
    return _source_value(bead, source_fields, "assignee")


def _assignee(bead: dict, source_fields: dict | None):
    return _assignment(bead, source_fields)[1]


def _issue_type(bead: dict, source_fields: dict | None):
    """Return the source issue type when restore remapped the Plug type."""
    present, value = _source_value(bead, source_fields, "issue_type")
    if present:
        return value
    return bead.get("type")


def _legacy_record(bead: dict, project: str, ontology) -> dict:
    """Flatten a Plug bead.get result to the public bd list JSON shape."""
    if not isinstance(bead, dict):
        raise AdapterError("bead.get returned a non-object bead")
    if bead.get("project") != project:
        raise AdapterError("bead.get escaped the bound project")
    ident = bead.get("id")
    kind = bead.get("type")
    if not isinstance(ident, str) or not ident or not isinstance(kind, str) or not kind:
        raise AdapterError("bead.get returned an invalid bead identity")
    fields = bead.get("fields")
    if not isinstance(fields, dict):
        raise AdapterError("bead.get omitted the bead fields object")
    source_fields = _source_fields(bead)

    result: dict = {"id": ident}
    # Restore can map the source issue_type to a different target type. The
    # retained source field/marker is authoritative, including explicit null.
    result["issue_type"] = _issue_type(bead, source_fields)

    # Copy source-only columns before the current writable projection. Mutable
    # lifecycle/assignment members are projected separately below, so import
    # provenance cannot silently override later ordinary writes.
    if isinstance(source_fields, dict):
        for name, value in source_fields.items():
            if name not in LEGACY_RESERVED_FIELDS:
                result[name] = value
    # Native status names are authoritative. Retained source status is import
    # provenance, and cannot rename a current status or undo a lifecycle write.
    result["status"] = bead["status"]
    for name, value in fields.items():
        if name not in LEGACY_RESERVED_FIELDS:
            result[name] = value

    assignment_present, assigned = _assignment(bead, source_fields)
    if assignment_present:
        result["assignee"] = assigned

    # The retained source map uses legacy snake_case timestamps. Fall back to
    # native Plug timestamps only for non-restored/workflow rows.
    for source_key, target_key in (("created_at", "created_at"), ("updated_at", "updated_at")):
        present, value = _source_value(bead, source_fields, source_key)
        if present and target_key not in fields:
            result[target_key] = value
    for source_key, target_key in CORE_TIME_FIELDS:
        if target_key == "closed_at" and not ontology.terminal(bead["status"]):
            continue
        if target_key not in result:
            _copy_if_present(result, bead, source_key, target_key)

    labels_present, source_labels = _source_value(bead, source_fields, "labels")
    if labels_present:
        result["labels"] = source_labels
    else:
        _copy_if_present(result, bead, "labels")

    # Only caller-owned metadata is returned. Restore provenance and Plug
    # allocator/control slots are intentionally excluded from the legacy shape.
    metadata = bead.get("metadata")
    ordinary_metadata = {
        key: value
        for key, value in metadata.items()
        if isinstance(key, str) and key != CLOSE_REASON_KEY
        and not any(key.startswith(prefix) for prefix in RESERVED_METADATA_PREFIXES)
    } if isinstance(metadata, dict) else {}
    source_metadata_present, source_metadata = _source_value(bead, source_fields, "metadata")
    if source_metadata_present:
        result["metadata"] = source_metadata
    elif ordinary_metadata:
        result["metadata"] = ordinary_metadata
    if not ontology.terminal(bead["status"]):
        result.pop("close_reason", None)
        result.pop("closed_at", None)
    else:
        _validate_lifecycle_projection(bead, ontology)
        typed = metadata.get(CLOSE_REASON_KEY) if isinstance(metadata, dict) else None
        if typed is not None:
            result["close_reason"] = typed["reason"]
    _copy_if_present(result, bead, "parent")
    return result


def _revision(item: dict) -> tuple[int, str]:
    value = item.get("revision")
    try:
        return int(value), str(item.get("id", ""))
    except (TypeError, ValueError):
        raise AdapterError("view.run returned an invalid revision")


def _read_items(client: Client, project: str, ontology) -> list[tuple[dict, dict]]:
    """Read every project row on one view snapshot, including custom statuses."""
    params = {
        "definition": {
            "filter": {},
            "fields": FIELDS,
            "sort": ["revision"],
        }
    }
    pairs: list[tuple[dict, dict]] = []
    cursor_seen: set[str] = set()
    stream_as_of = None
    while True:
        page = client.rpc("view.run", params)
        page_as_of = page.get("asOf")
        if not isinstance(page_as_of, dict) or page_as_of.get("incarnation") != client.incarnation:
            raise AdapterError("view cursor snapshot is not bound to the configured incarnation")
        if stream_as_of is None:
            stream_as_of = page_as_of
        elif page_as_of != stream_as_of:
            raise AdapterError("view cursor changed its asOf snapshot")
        items = page.get("items", [])
        if not isinstance(items, list):
            raise AdapterError("view.run items is not an array")
        for item in items:
            if not isinstance(item, dict):
                raise AdapterError("view.run returned a non-object item")
            if item.get("project") != project:
                raise AdapterError("view item escaped the bound project")
            _validate_lifecycle_projection(item, ontology)
            ident = item.get("id")
            if not isinstance(ident, str) or not ident:
                raise AdapterError("view.run returned a row without an id")
            detail = client.rpc("bead.get", {"id": {"id": ident}, "asOf": stream_as_of})
            bead = detail.get("bead")
            if not isinstance(bead, dict) or bead.get("id") != ident:
                raise AdapterError("bead.get result does not match the view row")
            # The pinned wire response keeps a live claim beside result.bead;
            # it is not a bead.assignee field. Attach only this validated
            # envelope member to the local projection input.
            lease = detail.get("lease")
            if lease is not None:
                if not isinstance(lease, dict) or not isinstance(lease.get("holder"), str) or not lease["holder"]:
                    raise AdapterError("bead.get returned an invalid lease envelope")
                bead = dict(bead)
                bead["lease"] = lease
            if bead.get("status") != _value(item, "status") or bead.get("type") != item.get("type"):
                raise AdapterError("bead.get result disagrees with the view row")
            pairs.append((item, bead))
        cursor = page.get("nextCursor")
        if not cursor:
            break
        if not isinstance(cursor, str) or cursor in cursor_seen:
            raise AdapterError("view cursor did not advance")
        cursor_seen.add(cursor)
        params["cursor"] = cursor
        params["asOf"] = stream_as_of
        if len(cursor_seen) > 1000:
            raise AdapterError("view cursor chain exceeded bound")
    if stream_as_of is None:
        raise AdapterError("view.run omitted its asOf snapshot")
    return pairs


def run(args: argparse.Namespace) -> int:
    client = Client(args.binding, args.project)
    # The mounted composition exposes identity through agent.brief, not the
    # catalog-only auth.whoami method. Validate it before the view reads.
    client.brief(1)
    ontology = client.ontology()
    pairs = _read_items(client, args.project, ontology)

    selected: list[tuple[tuple[int, str], dict]] = []
    for view_item, bead in pairs:
        status = bead.get("status")
        assignee = _assignee(bead, _source_fields(bead))
        source_fields = _source_fields(bead)
        # Canonical authenticated user IDs include the user: prefix. Preserve
        # that wire identity in output; legacy source assignees keep their exact
        # original string/null. A live claim also participates in selection.
        assigned = assignee in ("nikhil", "user:nikhil")
        if not ontology.terminal(status) and (assigned or _issue_type(bead, source_fields) == "decision"):
            selected.append((_revision(view_item), _legacy_record(bead, args.project, ontology)))
    selected.sort(key=lambda pair: pair[0])
    ontology.check_unchanged()
    json.dump([record for _, record in selected], sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True)
    parser.add_argument("--project", choices=("cody-ops", "signals-v2"), required=True)
    args = parser.parse_args()
    try:
        return run(args)
    except AdapterError as exc:
        print(f"plug catch-me-up adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
