// Artificial non-secret DOM and wire fixtures. This suite is preparation only.
import assert from "node:assert/strict";
import test from "node:test";
import { extractPresenceDOM, comparePresence, checkE3Dependencies } from "./presence_dom_adapter.mjs";

const anchor = "fef6e64de87826a6a20f245f298936457212897e";
const labelPrefix = "Client label (untrusted): ";
const clone = (value) => structuredClone(value);
const leaf = (value) => ({ textContent: value, get innerHTML() { throw new Error("Adapter must read textContent"); } });

// These are selector ports for unit inputs, not a page, handler or renderer.
function container(snapshot) {
  return { querySelectorAll(selector) {
    if (selector === ".elision") return snapshot.remainder.map(leaf);
    assert.equal(selector, ".presence-row");
    return snapshot.rows.map((row) => ({
      querySelector(s) {
        const key = { ".presence-name": "principalText", ".presence-identity": "identityText", ".presence-focus": "focusText", ".presence-scope": "scopeText" }[s];
        assert(key, "unexpected selector " + s);
        return row[key] === null ? null : leaf(row[key]);
      },
      querySelectorAll(s) { assert.equal(s, ".presence-display-name"); return row.labelTexts.map(leaf); },
    }));
  } };
}
function fixture(count = 50) {
  const presence = Array.from({ length: count }, (_, i) => ({
    sessionId: "artificial-session-" + (i + 1), principal: { kind: "anonymous", id: "anonymous" },
    displayName: "e3-agent-" + String(i + 1).padStart(2, "0"), focus: "bead/e3-" + String(i + 1).padStart(5, "0"),
    workingOn: ["bead/e3-" + String(i + 1).padStart(5, "0")],
  }));
  const snapshot = { rows: presence.slice(0, 5).map((r) => ({ principalText: "anonymous", identityText: "Anonymous principal", labelTexts: [labelPrefix + r.displayName], focusText: "Focus " + r.focus + " · Working on " + r.workingOn[0], scopeText: "1 working" })), remainder: count > 5 ? [(count - 5) + " more visible sessions are hidden to keep this view compact."] : [] };
  return { presence, sessions: clone(presence), snapshot };
}
const observe = (f) => comparePresence(f);
const hasIssue = (result, field) => result.issues.some((issue) => issue.field === field);
function assertMismatch(f, field) {
  const result = observe(f); assert.equal(result.matches, false, JSON.stringify(result));
  assert(hasIssue(result, field), JSON.stringify(result));
}

test("extracts the actual label, principal identity and working-resource selectors as text", () => {
  const f = fixture(); f.snapshot.rows[0].labelTexts = [labelPrefix + "<img src=x onerror=alert(1)>"];
  assert.deepEqual(extractPresenceDOM(container(f.snapshot)), f.snapshot);
});
test("fifty same-principal sessions retain five distinct visible labels and the 45 remainder", () => {
  const f = fixture(); const result = observe(f);
  assert.equal(result.matches, true, JSON.stringify(result));
  assert.equal(result.sessionCount, 50); assert.equal(result.visibleCount, 5);
  assert.equal(new Set(result.sessions.map((r) => r.sessionId)).size, 50);
});
test("equal principals and equal labels remain separate session rows", () => {
  const f = fixture(2);
  f.presence[1].displayName = f.sessions[1].displayName = f.presence[0].displayName;
  f.snapshot.rows[1].labelTexts = clone(f.snapshot.rows[0].labelTexts);
  assert.equal(observe(f).matches, true);
  f.snapshot.rows.pop(); assertMismatch(f, "dom.rows.length");
});
test("absent optional label retains the original anonymous unavailable message", () => {
  const f = fixture(1); delete f.presence[0].displayName; delete f.sessions[0].displayName;
  f.snapshot.rows[0].labelTexts = []; f.snapshot.rows[0].identityText = "Anonymous principal · client label unavailable";
  assert.equal(observe(f).matches, true);
});
test("a missing planned name is not inferred from agent metadata or principal", () => {
  const f = fixture(); delete f.presence[0].displayName;
  assertMismatch(f, "presence.displayName");
});
test("a label absent on the wire must also be absent in the DOM", () => {
  const f = fixture(1); delete f.presence[0].displayName; delete f.sessions[0].displayName;
  f.snapshot.rows[0].identityText = "Anonymous principal · client label unavailable";
  assertMismatch(f, "dom.labelTexts");
});
test("the exact untrusted-label prefix is required", () => {
  const f = fixture(); f.snapshot.rows[0].labelTexts = [f.presence[0].displayName]; assertMismatch(f, "dom.labelTexts");
});
test("a label cannot replace the separately displayed principal", () => {
  const f = fixture(); f.snapshot.rows[0].principalText = f.presence[0].displayName; assertMismatch(f, "dom.principalText");
});
test("delegation and principal identity remain separate from the label", () => {
  const f = fixture(1);
  for (const rows of [f.presence, f.sessions]) { rows[0].principal = { kind: "agent", id: "agent-a" }; rows[0].onBehalfOf = "sponsor-a"; }
  f.snapshot.rows[0].principalText = "on behalf of sponsor-a"; f.snapshot.rows[0].identityText = "agent:agent-a principal";
  assert.equal(observe(f).matches, true);
  f.snapshot.rows[0].identityText = "sponsor-a"; assertMismatch(f, "dom.identityText");
});
test("wire rows are correlated with actual opened sessions and preserve server order", () => {
  const f = fixture(); [f.snapshot.rows[0], f.snapshot.rows[1]] = [f.snapshot.rows[1], f.snapshot.rows[0]]; assertMismatch(f, "dom.labelTexts");
});
test("duplicate session IDs do not collapse into a same-principal identity", () => {
  const f = fixture(); f.presence[1].sessionId = f.presence[0].sessionId; assertMismatch(f, "presence.sessionId");
});
test("a foreign session is not accepted as one of the actual opened sessions", () => {
  const f = fixture(); f.presence[0].sessionId = "artificial-foreign-session"; assertMismatch(f, "presence.sessionId");
});
test("wire principal or working-resource substitution is detected", () => {
  let f = fixture(); f.presence[0].principal.id = "other"; assertMismatch(f, "presence.principal");
  f = fixture(); f.presence[0].workingOn = ["bead/other"]; assertMismatch(f, "presence.workingOn");
  f = fixture(); f.presence[0].focus = "bead/other"; assertMismatch(f, "presence.focus");
});
test("working-resource text and its count must match the wire", () => {
  let f = fixture(); f.snapshot.rows[0].focusText = "Focus bead/other · Working on bead/other"; assertMismatch(f, "dom.focusText");
  f = fixture(); f.snapshot.rows[0].scopeText = "99 working"; assertMismatch(f, "dom.scopeText");
});
test("three-resource bound retains an explicit remainder without inventing rows", () => {
  const f = fixture(1); const work = ["bead/a", "bead/b", "bead/c", "bead/d"];
  f.presence[0].workingOn = clone(work); f.sessions[0].workingOn = clone(work);
  f.snapshot.rows[0].focusText = "Focus " + f.presence[0].focus + " · Working on bead/a, bead/b, bead/c + 1 more";
  f.snapshot.rows[0].scopeText = "4 working"; assert.equal(observe(f).matches, true);
});
test("no-focus and no-work absence have explicit text", () => {
  const f = fixture(1);
  for (const rows of [f.presence, f.sessions]) { delete rows[0].focus; rows[0].workingOn = []; }
  f.snapshot.rows[0].focusText = "No focus shared · No claimed work shared"; f.snapshot.rows[0].scopeText = "0 working";
  assert.equal(observe(f).matches, true);
});
test("compact session remainder is checked exactly", () => {
  const f = fixture(); f.snapshot.remainder = ["44 more visible sessions are hidden to keep this view compact."]; assertMismatch(f, "dom.remainder");
});
test("missing selector fields and duplicated labels produce issues instead of inferred success", () => {
  for (const field of ["principalText", "identityText", "focusText", "scopeText"]) {
    const f = fixture(); f.snapshot.rows[0][field] = null; assertMismatch(f, "dom." + field);
  }
  const f = fixture(); f.snapshot.rows[0].labelTexts.push("Client label (untrusted): other"); assertMismatch(f, "dom.labelTexts");
});
test("malformed optional wire output is rejected rather than normalized by the adapter", () => {
  for (const value of ["", null, 4, " padded", "padded\u00a0", "x\u0000", "x\u200b", "\ud800", "x".repeat(65)]) {
    const f = fixture(1); f.presence[0].displayName = f.sessions[0].displayName = value;
    assertMismatch(f, "presence.displayName");
  }
  const f = fixture(1); f.presence[0].displayName = f.sessions[0].displayName = String.fromCodePoint(0x1f600).repeat(64);
  f.snapshot.rows[0].labelTexts = [labelPrefix + f.presence[0].displayName]; assert.equal(observe(f).matches, true);
});
test("malformed or absent required wire collections cannot pass", () => {
  let f = fixture(); delete f.presence[0].principal; assertMismatch(f, "presence.principal");
  f = fixture(); delete f.presence[0].workingOn; assertMismatch(f, "presence.workingOn");
  f = fixture(); f.presence = null; assert.equal(observe(f).matches, false);
});
test("missing admitted collab, webUI and combined pins refuse full runtime", () => {
  const result = checkE3Dependencies({ contractAnchor: anchor, webuiReference: "cbeeb1e52c639cf3823b4cb76a083359e08cf5eb" });
  assert.equal(result.status, "BLOCKED_MISSING_ADMISSION"); assert.equal(result.runtimeStatus, "UNRUN");
  assert(result.missing.includes("collab.head") && result.missing.includes("webui.admission") && result.missing.includes("combined.tree"));
});
test("complete artificial pin declarations do not claim runtime or admission proof", () => {
  const input = { contractAnchor: anchor, collab: { head: "1".repeat(40), admission: "artificial-only" }, webui: { head: "2".repeat(40), admission: "artificial-only" }, combined: { head: "3".repeat(40), tree: "4".repeat(40), admission: "artificial-only" } };
  const result = checkE3Dependencies(input); assert.equal(result.status, "DECLARED_PINS_PRESENT"); assert.equal(result.runtimeStatus, "UNRUN"); assert.equal(result.admissionVerified, false);
  input.contractAnchor = "5".repeat(40); assert(checkE3Dependencies(input).missing.includes("contractAnchor"));
});
