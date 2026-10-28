#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${UX_SPIKE_PORT:-4176}"
TMP="$(mktemp -d -t plug-ux-check.XXXXXX)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

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
  python3 - "$dom" "$view" "$budget" <<'PY'
import re
import sys
from html.parser import HTMLParser

path, view, budget_text = sys.argv[1:]
budget = int(budget_text)


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
expected_visible = budget if view in {"home", "next", "blocked", "changes"} else {"people": 5, "epic": 8, "catalog": 3}[view]
require(main_node.attrs.get("data-visible-count") == str(expected_visible), f"visible-node marker is not {expected_visible}")
candidate_count = int(main_node.attrs.get("data-candidate-count", "-1"))
require(candidate_count >= expected_visible, "candidate count is below visible count")
blocked_count = by_id("blocked-count")[0].text().replace(",", "")
require(blocked_count.isdigit() and int(blocked_count) > 0, "blocked navigation count is not a positive fixture metric")
main_text = main_node.text()
require(headings[view] in main_text, f"main view heading does not identify {view}")


def require_rows(class_name, minimum=1):
    rows = by_class(class_name)
    require(len(rows) >= minimum, f"expected at least {minimum} non-empty .{class_name} rows, got {len(rows)}")
    for index, row in enumerate(rows, 1):
        require(len(row.text()) >= 12, f".{class_name} row {index} is empty or too short")
    return rows


if view == "home":
    rows = require_rows("bead-title")
    require(len(rows) == budget, f"home rendered {len(rows)} rows, expected selected budget {budget}")
    ids = by_class("bead-id")
    require(len(ids) >= len(rows), "home rows are missing fixture ids")
    require(all(re.search(r"\bpl-\d{5}\b", node.text()) for node in ids[:len(rows)]), "home row is missing a bead id")
elif view == "next":
    rows = require_rows("queue-row")
    require(len(rows) == budget, f"next rendered {len(rows)} rows, expected selected budget {budget}")
    titles = by_class("queue-title")
    reasons = by_class("queue-reason")
    require(len(titles) >= len(rows) and all(node.text() for node in titles[:len(rows)]), "next row is missing a title")
    require(len(reasons) >= len(rows) and all(node.text() for node in reasons[:len(rows)]), "next row is missing its explanation")
    require(all(re.search(r"\bP[0-4]\b", row.text()) for row in rows), "next row is missing priority context")
elif view == "blocked":
    rows = require_rows("blocked-row")
    require(len(rows) == budget, f"blocked rendered {len(rows)} rows, expected selected budget {budget}")
    require(all(re.search(r"\bpl-\d{5}\b", row.text()) for row in rows), "blocked row is missing its blocker id")
    require(all(node.text() for node in by_class("blocked-owner")[:len(rows)]), "blocked row is missing its reason")
elif view == "changes":
    rows = require_rows("timeline-row")
    # A selected changes budget is an accounting promise.  Do not allow a
    # hidden presentation cap to claim that the full budget was shown.
    require(len(rows) == budget, f"changes rendered {len(rows)} rows, expected selected budget {budget}")
    require(all(re.search(r"\bpl-\d{5}\b", row.text()) for row in rows), "change row is missing its fixture id")
    require(all(node.text() for node in by_class("timeline-copy")[:len(rows)]), "change row is missing its description")
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
    "$CHROME" --headless --disable-gpu --no-sandbox --dump-dom \
      "http://127.0.0.1:${PORT}/?budget=${budget}&view=${view}" >"$dom" 2>/dev/null
    assert_rendered_view "$dom" "$view" "$budget"
  done
done

echo "UX spike browser checks passed for budgets 25, 50, and 100"
