#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${UX_SPIKE_PORT:-4176}"
TMP="$(mktemp -d -t plug-ux-check.XXXXXX)"
BROWSER_CHECK="$ROOT/spikes/ux/browser-check.mjs"
ORACLE="$ROOT/spikes/ux/fixture-oracle.py"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

test -f "$BROWSER_CHECK"
test -f "$ORACLE"

python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$ROOT/spikes/ux" >"$TMP/server.log" 2>&1 &
SERVER_PID=$!
for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null

CHROME="${CHROME_PATH:-/usr/bin/google-chrome}"

# The browser check intentionally inspects the rendered DOM rather than the
# source files.  A class-only count is too easy to satisfy with empty
# placeholders, so this parser checks that each rendered row has the content
# that makes it useful to a person.  Keep this dependency-free: Python's
# stdlib HTML parser is available wherever the capture helper runs.
assert_rendered_view() {
  local dom="$1"
  local view="$2"
  local budget="$3"
  python3 - "$dom" "$view" "$budget" "$ORACLE" <<'PY'
import re
import runpy
import sys
from html.parser import HTMLParser

path, view, budget_text, oracle_path = sys.argv[1:]
budget = int(budget_text)
oracle = runpy.run_path(oracle_path)
fixture = oracle["make_fixture"]()
oracle["validate_fixture"](fixture)
expected = oracle["expected_view"](fixture, view, budget)
fixture_by_id = {item["id"]: item for item in fixture}


class Node:
    def __init__(self, tag="", attrs=None, parent=None):
        self.tag = tag
        self.attrs = dict(attrs or [])
        self.parent = parent
        self.children = []
        self.parts = []

    def text(self):
        return " ".join(self.parts + [child.text() for child in self.children]).strip()


class DomParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("root")
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs, self.stack[-1])
        self.stack[-1].children.append(node)
        if tag not in {"meta", "link", "img", "input", "br", "hr"}:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if self.stack[-1].tag == tag:
            self.stack.pop()

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data):
        if data.strip():
            self.stack[-1].parts.append(data.strip())


with open(path, encoding="utf-8") as stream:
    parser = DomParser()
    parser.feed(stream.read())


def descendants(node):
    for child in node.children:
        yield child
        yield from descendants(child)


def has_class(node, name):
    return name in node.attrs.get("class", "").split()


def by_class(name):
    return [node for node in descendants(parser.root) if has_class(node, name)]


def by_id(name):
    return [node for node in descendants(parser.root) if node.attrs.get("id") == name]


def fail(message):
    raise SystemExit(f"{view}-{budget}: {message}")


def require(condition, message):
    if not condition:
        fail(message)


raw = " ".join(node.text() for node in descendants(parser.root))
require(by_class("app-shell"), "missing app shell")
require(by_id("main-view"), "missing main view")
require(by_id("snapshot-count"), "missing snapshot count")
require(by_id("blocked-count"), "missing relation-derived blocked navigation count")
snapshot = by_id("snapshot-count")[0].text()
require(snapshot == str(budget), f"snapshot count is {snapshot!r}, expected {budget}")
require(not re.search(r"\b(?:undefined|NaN|javascript error)\b", raw, re.I), "rendered an error value")
# The page is intentionally a local fixture preview. These positive markers
# make a regression toward a fake live-server claim fail loudly.
require(re.search(r"\bfixture-only\b", raw, re.I), "missing fixture-only state label")
require(re.search(r"\bfixture snapshot\b", raw, re.I), "missing fixture snapshot label")
require(re.search(r"\b\d+ fixture agents\b", raw, re.I), "missing fixture-agent count label")
require(re.search(r"\b(?:no live watch|watch transport not connected)\b", raw, re.I), "missing disconnected-watch label")
# These phrases claim a live connection or a larger simulation than this
# fixture-only page provides. Their accidental reintroduction should fail the
# browser gate until the UI labels the state honestly.
for stale in (
    r"\bbd serve connected\b",
    r"\bwatch\s*·\s*connected\b",
    r"\b50 simulated agents\b",
    r"\b45 more agents are quiet\b",
    r"\bupdates coalesced over 250 ms\b",
):
    require(not re.search(stale, raw, re.I), f"misleading fixture/server label: {stale}")

headings = {
    "home": "Changes since I looked",
    "next": "What next",
    "blocked": "Why blocked",
    "changes": "What changed",
    "people": "Who is on what",
    "epic": "Is the epic on track?",
    "catalog": "Declarative catalog",
}
main_node = by_id("main-view")[0]
require(main_node.attrs.get("data-view") == view, "main view data-view marker is wrong")
require(main_node.attrs.get("data-budget") == str(budget), "main view data-budget marker is wrong")
budget_buttons = [node for node in descendants(parser.root) if node.tag == "button" and node.attrs.get("data-budget") == str(budget)]
require(any("is-active" in node.attrs.get("class", "").split() for node in budget_buttons), "selected budget button is not active")
expected_visible = expected["visible_count"]
require(main_node.attrs.get("data-visible-count") == str(expected_visible), f"visible-node marker is not {expected_visible}")
candidate_count = int(main_node.attrs.get("data-candidate-count", "-1"))
require(candidate_count == expected["candidate_count"], f"candidate count is {candidate_count}, expected oracle count {expected['candidate_count']}")
blocked_count = by_id("blocked-count")[0].text().replace(",", "")
expected_blocked_count = sum(item["status"] == "blocked" for item in fixture)
require(blocked_count.isdigit() and int(blocked_count) == expected_blocked_count, "blocked navigation count does not match the fixture oracle")
main_text = main_node.text()
require(headings[view] in main_text, f"main view heading does not identify {view}")


def require_rows(class_name, minimum=1):
    rows = by_class(class_name)
    require(len(rows) >= minimum, f"expected at least {minimum} non-empty .{class_name} rows, got {len(rows)}")
    for index, row in enumerate(rows, 1):
        require(len(row.text()) >= 12, f".{class_name} row {index} is empty or too short")
    return rows


def status_for(node):
    classes = set(node.attrs.get("class", "").split())
    for child in descendants(node):
        classes.update(child.attrs.get("class", "").split())
    if "status-progress" in classes:
        return "in_progress"
    for status in ("open", "blocked", "closed"):
        if f"status-{status}" in classes:
            return status
    return ""


def assert_fixture_rows(rows, expected_rows, label):
    expected_ids = [item["id"] for item in expected_rows]
    require(len(expected_ids) == len(set(expected_ids)), f"{label} oracle contains duplicate IDs")
    actual_ids = [row.attrs.get("data-bead-id", "") for row in rows]
    require(actual_ids == expected_ids, f"{label} row IDs do not match the independent fixture oracle")
    require(len(actual_ids) == len(set(actual_ids)), f"{label} rendered duplicate bead IDs")
    for index, (row, item) in enumerate(zip(rows, expected_rows), 1):
        require(row.attrs.get("data-status") == item["status"], f"{label} row {index} status marker disagrees with oracle")
        require(status_for(row) == item["status"], f"{label} row {index} status signal disagrees with oracle")
        require(row.attrs.get("data-dependency-ids", "") == ",".join(item["dependency_ids"]), f"{label} row {index} dependencies disagree with oracle")
        require(row.attrs.get("data-unresolved-dependency-ids", "") == ",".join(item["unresolved_dependency_ids"]), f"{label} row {index} unresolved dependencies disagree with oracle")
        require(item["id"] in fixture_by_id, f"{label} row {index} is not in the oracle fixture")
        require((item["status"] == "blocked") == bool(item["unresolved_dependency_ids"]), f"{label} row {index} violates blocked relation invariant")


if view == "home":
    rows = require_rows("bead-row")
    require(len(rows) == budget, f"home rendered {len(rows)} rows, expected selected budget {budget}")
    assert_fixture_rows(rows, expected["rows"], "home")
    ids = by_class("bead-id")
    titles = by_class("bead-title")
    require(len(ids) >= len(rows) and len(titles) >= len(rows), "home rows are missing fixture content")
    for index, item in enumerate(expected["rows"]):
        require(ids[index].text() == f"{item['id']} · P{item['priority']}", f"home row {index + 1} ID or priority disagrees with oracle")
        require(titles[index].text() == item["title"], f"home row {index + 1} title disagrees with oracle")
elif view == "next":
    rows = require_rows("queue-row")
    require(len(rows) == budget, f"next rendered {len(rows)} rows, expected selected budget {budget}")
    assert_fixture_rows(rows, expected["rows"], "next")
    titles = by_class("queue-title")
    reasons = by_class("queue-reason")
    require(len(titles) >= len(rows) and all(node.text() for node in titles[:len(rows)]), "next row is missing a title")
    require(len(reasons) >= len(rows) and all(node.text() for node in reasons[:len(rows)]), "next row is missing its explanation")
    for index, item in enumerate(expected["rows"]):
        require(item["status"] == "open" and not item["unresolved_dependency_ids"], f"next oracle row {index + 1} is not ready")
        require(titles[index].text() == item["title"], f"next row {index + 1} title disagrees with oracle")
        require(reasons[index].text() == f"{item['reason']} · P{item['priority']}", f"next row {index + 1} reason disagrees with oracle")
elif view == "blocked":
    rows = require_rows("blocked-row")
    require(len(rows) == budget, f"blocked rendered {len(rows)} rows, expected selected budget {budget}")
    assert_fixture_rows(rows, expected["rows"], "blocked")
    ids = by_class("bead-id")
    reasons = by_class("blocked-owner")
    require(len(ids) >= len(rows) and len(reasons) >= len(rows), "blocked row is missing its relation content")
    for index, item in enumerate(expected["rows"]):
        require(item["status"] == "blocked" and item["unresolved_dependency_ids"], f"blocked oracle row {index + 1} lacks an unresolved relation")
        require(ids[index].text() == f"{item['id']} · blocked by {item['blocked_by']}", f"blocked row {index + 1} blocker IDs disagree with oracle")
        require(reasons[index].text() == item["reason"], f"blocked row {index + 1} reason disagrees with oracle")
elif view == "changes":
    rows = require_rows("timeline-row")
    # A selected changes budget is an accounting promise.  Do not allow a
    # hidden presentation cap to claim that the full budget was shown.
    require(len(rows) == budget, f"changes rendered {len(rows)} rows, expected selected budget {budget}")
    assert_fixture_rows(rows, expected["rows"], "changes")
    ids = by_class("timeline-actor")
    descriptions = by_class("timeline-copy")
    require(len(ids) >= len(rows) and len(descriptions) >= len(rows), "change row is missing its fixture content")
    for index, item in enumerate(expected["rows"]):
        require(ids[index].text() == item["id"], f"change row {index + 1} ID disagrees with oracle")
        require(item["change_kind"] in descriptions[index].text() and item["title"] in descriptions[index].text(), f"change row {index + 1} description disagrees with oracle")
elif view == "people":
    rows = require_rows("person-detail-row", minimum=5)
    require(len(rows) == 5, f"people rendered {len(rows)} rows, expected five fixture collaborators")
    require(all(node.text() for node in by_class("person-name")[:5]), "people row is missing a name")
    require(all(re.search(r"\b\d[\d,]* active\b", node.text()) for node in by_class("person-load")[:5]), "people row is missing active-work context")
elif view == "epic":
    rows = require_rows("epic-row", minimum=8)
    require(len(rows) == 8, f"epic rendered {len(rows)} rows, expected eight fixture epics")
    require(all(re.search(r"\b\d+/\d+\b", node.text()) for node in by_class("epic-count")[:8]), "epic row is missing a denominator")
    require(all(re.search(r"\b\d+% complete\b", node.text()) for node in by_class("epic-percent")[:8]), "epic row is missing completion context")
elif view == "catalog":
    rows = require_rows("catalog-spec", minimum=3)
    require(len(rows) == 3, f"catalog rendered {len(rows)} rows, expected three declarative examples")
    require(all(node.text() for node in by_class("catalog-code")[:3]), "catalog entry is missing its declaration")
    require(sum(1 for node in rows if node.attrs.get("data-catalog") or "Preview" in node.text()) >= 3, "catalog entries are missing preview actions")

PY
}

for budget in 25 50 100; do
  for view in home next blocked changes people epic catalog; do
    dom="$TMP/${view}-${budget}.html"
    console_log="$TMP/${view}-${budget}.console.log"
    if ! node "$BROWSER_CHECK" --chrome "$CHROME" \
      --url "http://127.0.0.1:${PORT}/?budget=${budget}&view=${view}" \
      --dom "$dom" >"$TMP/${view}-${budget}.browser.log" 2>"$console_log"; then
      cat "$console_log" >&2
      exit 1
    fi
    assert_rendered_view "$dom" "$view" "$budget"
  done
done

echo "UX spike browser checks passed for budgets 25, 50, and 100"
