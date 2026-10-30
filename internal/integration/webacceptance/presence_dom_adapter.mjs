// Acceptance-only adapter for cbeeb1e52c639cf3823b4cb76a083359e08cf5eb's DOM.
// Pure observations and dependency declarations; no server/browser/admission.
const contractAnchor = "fef6e64de87826a6a20f245f298936457212897e";
const labelPrefix = "Client label (untrusted): ";

// Self-contained so a later authorized CDP driver can evaluate this function
// against #presence-rows and return plain data. It reads text, never HTML.
export function extractPresenceDOM(container) {
  const read = (row, selector) => row.querySelector(selector)?.textContent ?? null;
  return {
    rows: Array.from(container.querySelectorAll(".presence-row"), (row) => ({
      principalText: read(row, ".presence-name"),
      identityText: read(row, ".presence-identity"),
      labelTexts: Array.from(row.querySelectorAll(".presence-display-name"), (node) => node.textContent),
      focusText: read(row, ".presence-focus"),
      scopeText: read(row, ".presence-scope"),
    })),
    remainder: Array.from(container.querySelectorAll(".elision"), (node) => node.textContent),
  };
}

const object = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
function equal(a, b) {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((value, i) => equal(value, b[i]));
  if (!object(a) || !object(b)) return false;
  const keys = Object.keys(a).sort();
  return equal(keys, Object.keys(b).sort()) && keys.every((key) => equal(a[key], b[key]));
}
function normalizedName(entry) {
  if (!Object.hasOwn(entry, "displayName")) return true;
  const name = entry.displayName;
  return typeof name === "string" && [...name].length >= 1 && [...name].length <= 64 &&
    !/[\p{Cc}\p{Cf}\p{Cs}]/u.test(name) && !/^\p{White_Space}|\p{White_Space}$/u.test(name);
}

// sessions is the expected mapping from real session.open/claim receipts,
// including the planned normalized displayName. It is not a trusted identity.
// DOM rows have no sessionId attribute: compare their preserved server order
// with actual wire rows. Never infer an independently visible session token.
export function comparePresence({ snapshot, presence, sessions } = {}) {
  const issues = [];
  const issue = (field, expected, observed, sessionId) => issues.push({ field, expected, observed, ...(sessionId ? { sessionId } : {}) });
  const finish = () => ({
    schema: "plug-e3-presence-observation/1", matches: issues.length === 0, issues,
    sessionCount: Array.isArray(presence) ? presence.length : null,
    visibleCount: Array.isArray(snapshot?.rows) ? snapshot.rows.length : null,
    sessions: Array.isArray(presence) ? presence : [], runtimeQualification: "UNRUN",
  });
  if (!Array.isArray(presence)) issue("presence", "array", typeof presence);
  if (!Array.isArray(sessions)) issue("sessions", "actual opened-session mapping", typeof sessions);
  if (!Array.isArray(snapshot?.rows) || !Array.isArray(snapshot?.remainder)) issue("dom", "extracted rows and remainder arrays", snapshot);
  if (issues.length) return finish();

  const expectedById = new Map();
  for (const expected of sessions) {
    if (!object(expected) || typeof expected.sessionId !== "string" || !expected.sessionId || expectedById.has(expected.sessionId)) {
      issue("sessions.sessionId", "unique nonempty session ID", expected?.sessionId);
    } else expectedById.set(expected.sessionId, expected);
  }
  if (presence.length !== sessions.length) issue("presence.length", sessions.length, presence.length);
  const seen = new Set();
  for (const entry of presence) {
    if (!object(entry)) { issue("presence.sessionId", "session object", entry); continue; }
    const id = entry.sessionId;
    if (typeof id !== "string" || !id || seen.has(id) || !expectedById.has(id)) issue("presence.sessionId", "unique actual opened session", id);
    seen.add(id);
    const expected = expectedById.get(id);
    if (!object(entry.principal) || typeof entry.principal.kind !== "string" || !entry.principal.kind || typeof entry.principal.id !== "string" || !entry.principal.id || (expected && !equal(entry.principal, expected.principal))) {
      issue("presence.principal", expected?.principal ?? "principal kind/id", entry.principal, id);
    }
    if (!normalizedName(entry) || (expected && entry.displayName !== expected.displayName)) issue("presence.displayName", expected?.displayName, entry.displayName, id);
    for (const field of ["focus", "onBehalfOf"]) {
      if ((entry[field] !== undefined && typeof entry[field] !== "string") || (expected && (entry[field] ?? "") !== (expected[field] ?? ""))) issue("presence." + field, expected?.[field], entry[field], id);
    }
    const working = entry.workingOn ?? [];
    if (!Array.isArray(working) || working.some((r) => typeof r !== "string" || !r) || (expected && !equal(working, expected.workingOn ?? []))) issue("presence.workingOn", expected?.workingOn, entry.workingOn, id);
  }

  const visible = presence.slice(0, 5);
  if (snapshot.rows.length !== visible.length) issue("dom.rows.length", visible.length, snapshot.rows.length);
  visible.forEach((entry, i) => {
    if (!object(entry)) return;
    const row = snapshot.rows[i];
    if (!object(row)) return; // The exact row-count mismatch above is retained.
    const principal = object(entry.principal) ? entry.principal : {};
    const kind = principal.kind, id = principal.id;
    const delegated = typeof entry.onBehalfOf === "string" ? entry.onBehalfOf : "";
    const name = delegated ? "on behalf of " + delegated : (id && id !== "anonymous" ? kind + ":" + id : kind);
    const identity = kind === "anonymous" && !delegated ? (entry.displayName ? "Anonymous principal" : "Anonymous principal · client label unavailable") : kind + ":" + id + " principal";
    const working = Array.isArray(entry.workingOn) ? entry.workingOn : [];
    const shown = working.slice(0, 3), hidden = working.length - shown.length;
    const work = shown.length ? "Working on " + shown.join(", ") + (hidden > 0 ? " + " + hidden + " more" : "") : "No claimed work shared";
    const expected = {
      principalText: name, identityText: identity,
      labelTexts: typeof entry.displayName === "string" && entry.displayName ? [labelPrefix + entry.displayName] : [],
      focusText: (entry.focus ? "Focus " + entry.focus : "No focus shared") + " · " + work,
      scopeText: working.length + " working",
    };
    for (const [field, value] of Object.entries(expected)) if (!equal(row[field], value)) issue("dom." + field, value, row[field], entry.sessionId);
  });
  const remainder = presence.length > 5 ? [(presence.length - 5) + " more visible sessions are hidden to keep this view compact."] : [];
  if (!equal(snapshot.remainder, remainder)) issue("dom.remainder", remainder, snapshot.remainder);
  return finish();
}

// Pin shape checks cannot verify admission. Even complete declarations do not
// authorize or run a workload. The controller supplies and verifies provenance
// in a separate runtime intake; a reference/local source SHA is insufficient.
export function checkE3Dependencies(input = {}) {
  const pins = object(input) ? input : {};
  const missing = [];
  if (pins.contractAnchor !== contractAnchor) missing.push("contractAnchor");
  for (const role of ["collab", "webui", "combined"]) {
    const value = object(pins[role]) ? pins[role] : {};
    if (typeof value.head !== "string" || !/^[a-f0-9]{40}$/.test(value.head)) missing.push(role + ".head");
    if (typeof value.admission !== "string" || !value.admission.trim()) missing.push(role + ".admission");
    if (role === "combined" && (typeof value.tree !== "string" || !/^[a-f0-9]{40}$/.test(value.tree))) missing.push("combined.tree");
  }
  return {
    schema: "plug-e3-runtime-dependencies/1",
    status: missing.length ? "BLOCKED_MISSING_ADMISSION" : "DECLARED_PINS_PRESENT",
    missing, contractAnchor, admissionVerified: false, runtimeStatus: "UNRUN",
    requiredStoredBeads: 20000, requiredDistinctSessions: 50,
    taskNames: ["what next", "why blocked", "what changed", "who is on what", "epic progress"],
    qualification: "LOCAL BLOCK; security UNKNOWN; separate admitted runtime intake required",
  };
}
