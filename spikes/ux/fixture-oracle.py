#!/usr/bin/env python3
"""Independent deterministic oracle for the UX fixture browser checks.

This intentionally duplicates the fixture contract in a separate language and
file. It never imports or reads app.js, so a browser mutation that repeats or
fabricates rows cannot make the checker agree with the page by construction.
"""

from __future__ import annotations

import json
import sys
from typing import Any


EPICS = [
    "Shared server readiness",
    "Agent handoff",
    "Query budget",
    "History and restore",
    "Authorization chain",
    "Plugin catalog",
    "Dolt standby",
    "CLI parity",
]
TITLE_PARTS = [
    "Add cursor expiry notice",
    "Document lease recovery",
    "Tighten ready ordering",
    "Expose revision in list row",
    "Verify project stamp",
    "Trim duplicate watcher events",
    "Pin migration checkpoint",
    "Add blocked reason",
    "Review plugin grant",
    "Refresh API examples",
]
CHANGE_KINDS = [
    "status changed",
    "comment added",
    "claim renewed",
    "dependency linked",
    "priority raised",
]


def _pick(values: list[str], index: int) -> str:
    return values[index % len(values)]


def make_fixture() -> list[dict[str, Any]]:
    seed = 0x51A7
    statuses: list[str] = []
    fixture: list[dict[str, Any]] = []
    for index in range(20_000):
        seed = (seed * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
        random_value = seed / 4_294_967_296
        candidate_status = (
            "in_progress"
            if random_value < 0.39
            else "open"
            if random_value < 0.86
            else "closed"
        )
        dependency_indexes: list[int] = []
        if index >= 1 and index % 3 == 0:
            dependency_indexes.append(index - 1)
        if index >= 7 and index % 11 == 0:
            dependency_indexes.append(index - 7)
        if index >= 31 and index % 37 == 0:
            dependency_indexes.append(index - 31)
        dependency_ids = [fixture[target]["id"] for target in dependency_indexes]
        unresolved_dependency_ids = [
            fixture[target]["id"]
            for target in dependency_indexes
            if statuses[target] != "closed"
        ]
        status = "blocked" if unresolved_dependency_ids else candidate_status
        epic = _pick(EPICS, index * 11 + 2)
        owner_number = (index + index // 23) % 50 + 1
        owner_id = f"agent-{owner_number:02d}"
        bead_id = f"pl-{index + 1:05d}"
        reason = (
            f"waiting on {', '.join(unresolved_dependency_ids)}"
            if unresolved_dependency_ids
            else f"ready after {', '.join(dependency_ids)}"
            if dependency_ids
            else "ready with no dependencies"
        )
        relations = [{"type": "depends_on", "targetId": target} for target in dependency_ids]
        relations.append({"type": "belongs_to_epic", "targetId": epic})
        changed_minutes = (index * 37) % 960 + index // 960
        item = {
            "id": bead_id,
            "title": f"{_pick(TITLE_PARTS, index * 13 + index // 17 + 3)} · {epic}",
            "status": status,
            "priority": (index * 3) % 5,
            "owner_id": owner_id,
            "epic": epic,
            "relations": relations,
            "dependency_ids": dependency_ids,
            "unresolved_dependency_ids": unresolved_dependency_ids,
            "blocked_by": ", ".join(unresolved_dependency_ids),
            "reason": reason,
            "changed_minutes": changed_minutes,
            "change_kind": _pick(CHANGE_KINDS, index * 3 + 1),
        }
        fixture.append(item)
        statuses.append(status)

    return fixture


def _ordered_items(fixture: list[dict[str, Any]], view: str) -> list[dict[str, Any]]:
    if view == "home":
        items = [item for item in fixture if item["changed_minutes"] < 90]
        return sorted(items, key=lambda item: item["changed_minutes"])
    if view == "next":
        items = [
            item
            for item in fixture
            if item["status"] == "open" and not item["unresolved_dependency_ids"]
        ]
        return sorted(
            items,
            key=lambda item: (
                item["priority"],
                len(item["dependency_ids"]),
                item["changed_minutes"],
            ),
        )
    if view == "blocked":
        return sorted(
            [item for item in fixture if item["status"] == "blocked"],
            key=lambda item: (item["changed_minutes"], item["priority"]),
        )
    if view == "changes":
        return sorted(fixture, key=lambda item: item["changed_minutes"])
    raise ValueError(f"no bead rows for {view}")


def expected_view(fixture: list[dict[str, Any]], view: str, budget: int) -> dict[str, Any]:
    if view in {"home", "next", "blocked", "changes"}:
        ordered = _ordered_items(fixture, view)
        return {
            "candidate_count": len(ordered),
            "visible_count": min(len(ordered), budget),
            "rows": ordered[:budget],
        }
    compact_counts = {"people": 5, "epic": 8, "catalog": 3}
    return {
        "candidate_count": compact_counts[view],
        "visible_count": compact_counts[view],
        "rows": [],
    }


def validate_fixture(fixture: list[dict[str, Any]]) -> None:
    by_id = {item["id"]: item for item in fixture}
    if len(by_id) != len(fixture):
        raise AssertionError("fixture bead IDs are not unique")
    for item in fixture:
        dependencies = [
            relation["targetId"]
            for relation in item["relations"]
            if relation["type"] == "depends_on"
        ]
        if dependencies != item["dependency_ids"]:
            raise AssertionError(f"dependency relation mismatch for {item['id']}")
        if any(target not in by_id for target in dependencies):
            raise AssertionError(f"unknown dependency target for {item['id']}")
        unresolved = [
            target for target in dependencies if by_id[target]["status"] != "closed"
        ]
        if unresolved != item["unresolved_dependency_ids"]:
            raise AssertionError(f"unresolved dependency mismatch for {item['id']}")
        if (item["status"] == "blocked") != bool(unresolved):
            raise AssertionError(f"blocked status mismatch for {item['id']}")
        epic_relations = [
            relation
            for relation in item["relations"]
            if relation["type"] == "belongs_to_epic"
        ]
        if len(epic_relations) != 1 or epic_relations[0]["targetId"] != item["epic"]:
            raise AssertionError(f"epic relation mismatch for {item['id']}")


if __name__ == "__main__":
    fixture = make_fixture()
    validate_fixture(fixture)
    json.dump(fixture, sys.stdout, separators=(",", ":"))
