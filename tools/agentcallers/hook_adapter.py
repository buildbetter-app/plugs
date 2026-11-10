#!/usr/bin/env python3
"""Prepared Claude/Codex lifecycle hook adapter.

The five census commands share one public hook contract: read one canonical
provider event JSON object, refresh Plug's compact context, and emit the exact
hookSpecificOutput envelope used by bd prime --hook-json. The event name and
matcher remain owned by caller settings templates; this process never changes
live settings.
"""

from __future__ import annotations

import argparse
import json
import sys

from binding import AdapterError, Client


EVENTS = {"PostCompact", "PreCompact", "SessionStart", "UserPromptSubmit"}
CLOSE_REASON_KEY = "i8.host.close_reason"
CLOSE_REASON_SCHEMA = "plug-host-close-reason/1"
EVENT_POSITION_KEYS = ("cursor", "incarnation", "revision", "index")
DURABLE_DETAIL_KEYS = (
    "changed",
    "before",
    "after",
    "principal",
    "chain",
    "setBy",
    "cause",
    "derived",
    "details",
)


def _field(value, name, default=None):
    if isinstance(value, dict):
        return value.get(name, default)
    return default


def _typed_close_reason(row: dict, ontology) -> str | None:
    """Project lifecycle resolution only from the shared typed metadata slot.

    The retained writers put CloseOp and this SetMeta value in one CAS guarded
    transaction. A reopened row must not carry the old resolution, and an
    ordinary comment/direct close_reason field is never parsed as a reason.
    """
    status = _field(row, "status")
    metadata = row.get("metadata")
    typed = metadata.get(CLOSE_REASON_KEY) if isinstance(metadata, dict) else None
    if not ontology.terminal(status):
        return None
    if typed is None:
        return None
    if not isinstance(typed, dict) or typed.get("schema") != CLOSE_REASON_SCHEMA or not isinstance(typed.get("reason"), str):
        raise AdapterError("closed context row has an invalid typed close reason")
    return typed["reason"]


def _render_bead(row: dict, ontology) -> str:
    resolution = _typed_close_reason(row, ontology)
    ident = row.get("id", "?")
    fields = _field(row, "fields", {})
    field_title = fields.get("title", "") if isinstance(fields, dict) else ""
    text = row.get("title") or field_title
    suffix = f" (resolution: {resolution})" if resolution is not None else ""
    return f"- {ident}: {text}{suffix}".rstrip()


def _event_position(event: dict) -> dict:
    position = {}
    for key in EVENT_POSITION_KEYS:
        if key not in event:
            raise AdapterError(f"recentChanges event is missing position member {key}")
        position[key] = event[key]
    return position


def _render_event(event: dict) -> str:
    """Render the real durable/control Event union without dropping its plane."""
    category = event.get("category")
    if category not in {"durable", "control"}:
        raise AdapterError("recentChanges contains a non-Event row")
    event_type = event.get("type")
    if not isinstance(event_type, str) or not event_type:
        raise AdapterError("recentChanges event has no type")
    position = _event_position(event)
    resource = event.get("resource", "")
    if resource is None:
        resource = ""
    if not isinstance(resource, str):
        raise AdapterError("recentChanges event resource is not a string")
    if category == "durable":
        details = {key: event[key] for key in DURABLE_DETAIL_KEYS if key in event}
        if not resource:
            raise AdapterError("durable recentChanges event has no resource")
    else:
        details = event.get("details")
        if not isinstance(details, dict):
            raise AdapterError("control recentChanges event has no details object")
        # Keep the control envelope's explicit derived marker alongside the
        # tagged details object, just as the wire union supplies it.
        if "derived" in event:
            details = {"details": details, "derived": event["derived"]}
    position_json = json.dumps(position, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    details_json = json.dumps(details, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    resource_text = resource or "(none)"
    return f"- {category} type={event_type} resource={resource_text} position={position_json} details={details_json}"


def render_context(brief: dict, project: str, ontology) -> str:
    """Render all caller-visible brief planes, including durable event unions."""
    lines = [f"# Plug context ({project})"]
    who = _field(brief, "whoami", {})
    principal = _field(who, "principal", {})
    lines.append(f"Principal: {_field(principal, 'id', 'unknown')}")
    for title, key in (("Claims", "claims"), ("Ready", "readyTop")):
        rows = brief.get(key, [])
        lines.append(f"\n## {title}")
        if not rows:
            lines.append("(none)")
        else:
            for row in rows:
                if isinstance(row, dict):
                    lines.append(_render_bead(row, ontology))
                else:
                    lines.append(f"- {row}")
    events = brief.get("recentChanges")
    if not isinstance(events, list):
        raise AdapterError("brief recentChanges is not an array")
    lines.append("\n## Recent changes")
    if not events:
        lines.append("(none)")
    else:
        for event in events:
            if not isinstance(event, dict):
                raise AdapterError("recentChanges contains a non-object event")
            lines.append(_render_event(event))

    ontology = brief.get("ontologySummary")
    if not isinstance(ontology, dict):
        raise AdapterError("brief ontologySummary is not an object")
    lines.append("\n## Ontology summary")
    lines.append(json.dumps(ontology, ensure_ascii=False, sort_keys=True, separators=(",", ":")))

    lines.append("\n## Conventions")
    lines.append(json.dumps(brief.get("conventions", {}), ensure_ascii=False, sort_keys=True))
    lines.append("\n## Memories")
    memories = brief.get("memories", [])
    if not memories:
        lines.append("(none)")
    else:
        for memory in memories:
            if isinstance(memory, dict):
                lines.append(f"- {memory.get('key', memory.get('id', '?'))}: {memory.get('content', '')}")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> int:
    if args.event not in EVENTS:
        raise AdapterError("unsupported hook event")
    raw = sys.stdin.read()
    if raw.strip():
        try:
            event = json.loads(raw)
        except ValueError as exc:
            raise AdapterError("hook input is not JSON") from exc
        if not isinstance(event, dict):
            raise AdapterError("hook input must be a JSON object")
        supplied = event.get("hook_event_name") or event.get("hookEventName")
        # The canonical provider payload has additional event-specific members
        # (session, cwd, transcript, reason, source, and model). They are
        # intentionally ignored after the event-name check: the adapter is a
        # context refresh and preserves the source hook envelope regardless of
        # provider-only payload extensions.
        if supplied and supplied != args.event:
            raise AdapterError("hook event input does not match configured event")
    client = Client(args.binding, args.project)
    ontology = client.ontology()
    brief = client.brief(args.budget_tokens)
    context = render_context(brief, args.project, ontology)
    ontology.check_unchanged()
    output = {"hookSpecificOutput": {"hookEventName": args.event, "additionalContext": context}}
    json.dump(output, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True)
    parser.add_argument("--project", choices=("cody-ops", "signals-v2"), required=True)
    parser.add_argument("--provider", choices=("claude", "codex"), required=True)
    parser.add_argument("--event", required=True)
    parser.add_argument("--budget-tokens", type=int, default=2048)
    args = parser.parse_args()
    if args.budget_tokens < 1:
        parser.error("--budget-tokens must be positive")
    try:
        return run(args)
    except AdapterError as exc:
        print(f"plug hook adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
