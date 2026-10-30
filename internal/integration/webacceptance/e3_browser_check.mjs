// Deterministic E3 observations against the actual embedded UI and real RPC.
// No intercepted requests, frontend rows, injected product state or npm deps.
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Recorder evidence only. The optional wire shape is owned by the
// directBlockers transport; this check cannot supply a missing capability.
export function assessWhyBlockedEvidence({ wire, startedAt, project, viewId, itemId, blockerId, explanation }) {
  const blocked = (problem) => ({ observed: false, problem });
  const object = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
  const closed = (value, keys) => object(value) && Object.keys(value).every((key) => keys.includes(key));
  const ref = (value) => closed(value, ["id", "branch", "key"]) && (
    (typeof value.id === "string" && value.id.length > 0 && !Object.hasOwn(value, "key") &&
      (!Object.hasOwn(value, "branch") || (Number.isSafeInteger(value.branch) && value.branch >= 0))) ||
    (typeof value.key === "string" && value.key.length > 0 && !Object.hasOwn(value, "id") && !Object.hasOwn(value, "branch")));
  const safeReasons = [
    "blocker reference is unresolved", "blocker is missing from the snapshot",
    "blocker ontology is unavailable in the snapshot", "blocker status is unknown to its project ontology",
    "blocker status does not satisfy blocks", "blocker status does not satisfy conditional-blocks",
  ];
  let match;
  for (const [index, entry] of (Array.isArray(wire) ? wire : []).entries()) {
    let request;
    try { request = JSON.parse(entry.request?.postData); } catch { continue; }
    if (entry.at >= startedAt && request.jsonrpc === "2.0" && typeof request.id === "string" && request.id &&
        request.method === "view.explain" && request.params?.viewId === viewId &&
        request.params?.itemId === itemId && request.params?._meta?.project === project) match = { index, entry, request };
  }
  if (!match) return blocked("no fresh matching browser view.explain request");
  let response;
  try { response = JSON.parse(match.entry.rawResponse); } catch { return blocked("matching view.explain response body is absent or malformed"); }
  if (match.entry.failure || match.entry.response?.status !== 200 || response?.jsonrpc !== "2.0" ||
      response.id !== match.request.id || Object.hasOwn(response, "error")) return blocked("matching view.explain did not succeed");
  const result = response.result;
  if (!object(result) || result.viewId !== viewId || result.itemId !== itemId || result.included !== true) return blocked("view.explain does not include the matching dependent");
  const verdict = result.directBlockers;
  if (!closed(verdict, ["edges", "hiddenCount"]) || !Array.isArray(verdict.edges) || verdict.edges.length > 100 ||
      !Number.isInteger(verdict.hiddenCount) || verdict.hiddenCount < 0 || verdict.hiddenCount > 100) return blocked("directBlockers capability is absent or malformed");
  for (const edge of verdict.edges) {
    if (!closed(edge, ["blocker", "kind", "state", "reason"]) || !ref(edge.blocker) ||
        !["blocks", "conditional-blocks"].includes(edge.kind) || !["satisfied", "unsatisfied", "unknown"].includes(edge.state) ||
        (Object.hasOwn(edge, "reason") && (typeof edge.reason !== "string" || [...edge.reason].length > 128 || !safeReasons.includes(edge.reason)))) {
      return blocked("directBlockers edge/reference/reason is malformed or not producer-safe");
    }
  }
  const edge = verdict.edges.find((edge) => edge.blocker.id === blockerId && (edge.blocker.branch ?? 0) === 0 &&
    edge.state === "unsatisfied" && edge.reason === "blocker status does not satisfy " + edge.kind);
  if (!edge) return blocked("no server-linked readable unsatisfied fixture blocker with a safe reason");
  // Text is corroboration only, after the actual linked wire edge is proven.
  // The caller supplies visible innerText from the existing explanation region.
  const renderedRefs = typeof explanation === "string" ? explanation.match(/[\p{L}\p{N}_./-]+/gu) || [] : [];
  const unavailable = typeof explanation === "string" && /No direct (?:blocker|exclusion) reason was supplied by the server|Explanation unavailable\./.test(explanation);
  if (typeof explanation !== "string" || unavailable || !renderedRefs.includes(blockerId) || !explanation.includes(edge.reason)) {
    return blocked("linked blocker reference and safe reason are not both rendered");
  }
  return { observed: true, wireIndex: match.index, blocker: edge.blocker, kind: edge.kind, reason: edge.reason };
}

async function runBrowser() {
const [base, evidence, chromePath] = process.argv.slice(2);
const fixture = JSON.parse(await readFile(path.join(evidence, "browser-input.json"), "utf8"));
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-e3-browser-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
const names = ["what next", "why blocked", "what changed", "who is on what", "epic progress"];
const tasks = names.map((name) => ({ name, status: "NOT_RUN", elapsedMs: null, observations: [], requiredFields: [], missingFields: [] }));
const receipt = {
  schema: "plug-e3-browser/1", qualification: "BLOCKED", sourceHead: fixture.sourceHead, sourceTree: fixture.sourceTree,
  storedBeads: fixture.storedBeads, simulatedAgents: fixture.agents.length,
  taskOrder: names, tasks, infrastructure: {},
  timingMeaning: "wall-clock elapsed script time; operational waits are not usability thresholds",
  policyScope: "fixture-only explicit current stored grants; production saved-view resolver and installed current policy not qualified",
};
let stderr = "", ws, send, evaluate;
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description, timeout = 25000) {
  const started = performance.now();
  while (performance.now() - started < timeout) {
    const value = await fn();
    if (value) return value;
    await delay(50);
  }
  throw new Error("Timed out: " + description);
}
const calls = [], browserWire = [], consoleErrors = [], egress = [], requests = new Map();
const pendingBodies = new Set();
async function direct(method, params, headers = {}) {
  const request = { jsonrpc: "2.0", id: "e3-harness-" + (calls.length + 1), method, params };
  const started = performance.now();
  const response = await fetch(base + "/rpc/v1", { method: "POST", headers: { "Content-Type": "application/json", "Plug-RPC": "1", ...headers }, body: JSON.stringify(request), signal: AbortSignal.timeout(30000) });
  const raw = await response.text();
  const reply = JSON.parse(raw);
  calls.push({ request, status: response.status, rawResponse: raw, response: reply, elapsedMs: performance.now() - started });
  return reply;
}
const meta = (idempotencyKey) => ({ project: fixture.project, ...(idempotencyKey ? { idempotencyKey } : {}) });
const text = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ""`);
const click = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)})?.click()`);
const domRows = (selector) => evaluate(`Array.from(document.querySelectorAll(${JSON.stringify(selector)})).map(r => ({text:r.textContent,title:r.querySelector('.row-title')?.textContent,meta:r.querySelector('.row-meta')?.textContent,revision:r.querySelector('.row-revision')?.textContent}))`);
const settled = () => until(async () => await text("#connection-state") !== "Loading" && await text("#connection-state") !== "Connecting" && await evaluate("document.querySelector('#refresh') && !document.querySelector('#refresh').disabled"), "completed UI read");
async function select(view) { await click(`[data-view="${view}"]`); await settled(); }
async function screenshot(name) {
  const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
  await writeFile(path.join(evidence, name + ".png"), Buffer.from(png.data, "base64"));
}
async function task(index, requiredFields, fn) {
  const outcome = tasks[index];
  outcome.requiredFields = requiredFields;
  const started = performance.now();
  try {
    await fn(outcome);
    outcome.status = outcome.missingFields.length ? "BLOCKED" : "OBSERVED";
  } catch (error) {
    outcome.status = /Timed out|timeout/i.test(error.message) ? "TIMEOUT" : "FAILED";
    outcome.error = String(error.stack || error);
  } finally {
    outcome.elapsedMs = performance.now() - started;
    outcome.browserWireIndexes = browserWire.map((entry, i) => ({ entry, i })).filter(({ entry }) => entry.at >= outcome.startedAt).map(({ i }) => i);
    if (evaluate) outcome.finalDOM = await evaluate("document.body.innerText").catch((e) => String(e));
    await screenshot("task-" + (index + 1)).catch((error) => { outcome.screenshotError = String(error); });
    await writeFile(path.join(evidence, "browser-outcomes.json"), JSON.stringify(receipt, null, 2));
    console.log("E3 " + outcome.name + ": " + outcome.status + " " + outcome.elapsedMs.toFixed(3) + " ms");
  }
}
try {
  const debug = await until(() => stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1], "actual Chrome startup");
  const address = "http://" + new URL(debug).host;
  const version = await (await fetch(address + "/json/version")).json();
  receipt.infrastructure.browser = version.Browser;
  receipt.infrastructure.node = process.version;
  const target = await (await fetch(address + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map();
  send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error(JSON.stringify(event.error))) : promise.resolve(event.result);
      return;
    }
    if (event.method === "Runtime.exceptionThrown") consoleErrors.push(event.params.exceptionDetails);
    if (event.method === "Runtime.consoleAPICalled" && event.params.type === "error") consoleErrors.push(event.params);
    if (event.method === "Network.requestWillBeSent") {
      const { requestId, request } = event.params;
      if (/^https?:/.test(request.url) && new URL(request.url).origin !== new URL(base).origin) egress.push(request.url);
      if (request.url === base + "/rpc/v1" || request.url === base + "/watch/v1") {
        const entry = { at: performance.now(), requestId, request };
        browserWire.push(entry); requests.set(requestId, entry);
      }
    }
    if (event.method === "Network.responseReceived" && requests.has(event.params.requestId)) requests.get(event.params.requestId).response = event.params.response;
    if (event.method === "Network.loadingFailed" && requests.has(event.params.requestId)) requests.get(event.params.requestId).failure = event.params;
    if (event.method === "Network.loadingFinished" && requests.has(event.params.requestId)) {
      const entry = requests.get(event.params.requestId);
      const result = send("Network.getResponseBody", { requestId: event.params.requestId }).then((body) => {
        entry.rawResponse = body.base64Encoded ? Buffer.from(body.body, "base64").toString() : body.body;
      }).catch((error) => { entry.bodyError = String(error); }).finally(() => pendingBodies.delete(result));
      pendingBodies.add(result);
    }
  });
  evaluate = async (expression) => {
    const value = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (value.exceptionDetails) throw new Error(JSON.stringify(value.exceptionDetails));
    return value.result?.value;
  };
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable", { maxTotalBufferSize: 16000000 });
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  const started = performance.now();
  await send("Page.navigate", { url: base + "/ui/" });
  await until(() => evaluate("document.readyState === 'complete' && !!document.querySelector('#refresh')"), "actual embedded page load");
  await settled();
  receipt.infrastructure.initialReadElapsedMs = performance.now() - started;
  receipt.infrastructure.initialConnection = await text("#connection-state");
  receipt.infrastructure.initialWatch = await text("#watch-state");

  await task(0, ["bead.ready.items[].id/type/project/status/revision/fields.title", "bead.ready.asOf", "bead.ready.nextCursor", "dependency-aware membership", "bounded continuation"], async (outcome) => {
    outcome.startedAt = performance.now();
    await select("next");
    assert.equal(await text("#connection-state"), "Connected");
    const rows = await domRows("#next-rows .row");
    outcome.observations.push({ rows, continuation: await text("#next-more"), snapshot: await text("#snapshot-state") });
    assert(rows.length > 0 && rows.length <= 25);
    assert(rows[0].text.includes(fixture.blockerId));
    assert(!rows.some((row) => row.text.includes(fixture.blockedId)));
    assert(!rows.some((row) => row.text.includes("e3-00001")));
    assert.match(await text("#next-more"), /more|continue|page/i);
    outcome.answer = "Unclaimed blocker e3-00051 is first; dependent e3-00052 and the 50 live claims are excluded by the real ready service.";
    outcome.observations.push("Ready uses the admitted core service in anonymous no-plugin mode; fixture policy qualification applies to views/watch/presence, not baseline ready authorization.");
  });

  await task(1, ["view.run.items[].id/status", "view.explain.reasons", "direct readable blocker reference and reason", "hidden-blocker count only for unreadable causes"], async (outcome) => {
    outcome.startedAt = performance.now();
    await select("blocked");
    assert.equal(await text("#connection-state"), "Connected");
    const blocked = await text("#blocked-state");
    assert(blocked.includes(fixture.blockedId));
    await select("explain");
    const explanationStartedAt = performance.now();
    await evaluate(`document.querySelector('#explain-id').value=${JSON.stringify(fixture.blockedId)};document.querySelector('#explain-form').requestSubmit()`);
    await settled();
    await Promise.allSettled([...pendingBodies]);
    const explanation = await evaluate("document.querySelector('#explain-state')?.innerText || ''");
    const blockerEvidence = assessWhyBlockedEvidence({ wire: browserWire, startedAt: explanationStartedAt,
      project: fixture.project, viewId: fixture.blockedViewId, itemId: fixture.blockedId, blockerId: fixture.blockerId, explanation });
    outcome.observations.push({ blocked, explanation, blockerEvidence });
    if (!blockerEvidence.observed) {
      outcome.missingFields.push(blockerEvidence.problem + "; blocked candidate is explicitly selected by stored ID, not a supported blocked predicate");
    }
  });

  await task(2, ["view.run.items[].id/revision/fields.title", "view.run.asOf.revision/incarnation", "view.watch.subscriptionId/cursor", "NDJSON subscriptionId/events/cursor", "watch.delta.details.fromCursor/toCursor/events", "stable survivor order"], async (outcome) => {
    outcome.startedAt = performance.now();
    await select("home");
    assert.equal(await text("#connection-state"), "Connected");
    await until(async () => /connected/i.test(await text("#watch-state")), "real view.watch NDJSON attachment");
    const before = await domRows("#changed-rows .row");
    assert(before.length > 0 && before.length <= 25);
    assert(!before.some((row) => row.text.includes(fixture.deniedId)));
    const newTitle = "E3 real browser observed change from e3-agent-01";
    const updated = await direct("bead.update", { id: { id: "e3-00001", branch: 0 }, fields: { title: newTitle }, _meta: { ...meta(fixture.updateKey), agent: "e3-agent-01" } });
    assert.equal(updated.error, undefined, JSON.stringify(updated));
    await until(async () => (await text("#changed-rows")).includes(newTitle), "actual watch delta followed by view.run");
    const after = await domRows("#changed-rows .row");
    assert.deepEqual(after.map((row) => row.meta), before.map((row) => row.meta));
    outcome.observations.push({ before, after, watch: await text("#watch-state"), snapshot: await text("#snapshot-state"), update: calls.at(-1) });
    outcome.answer = newTitle;
  });

  await task(3, ["presence[].sessionId/principal/displayName/focus/workingOn", "distinct simulated agent displayName identity", "compact visible rows and explicit remainder"], async (outcome) => {
    outcome.startedAt = performance.now();
    await select("presence");
    await until(async () => {
      const stats = await (await fetch(base + "/fixture/e3/stats")).json();
      return stats.sessions === 50 && stats.streams === 0 && stats.watches === 0;
    }, "browser watch/session release before 50-agent presence observation");
    await click("#refresh"); await settled();
    const presence = await evaluate("Array.from(document.querySelectorAll('#presence-rows .presence-row')).map(row=>({name:row.querySelector('.presence-name').textContent,label:row.querySelector('.presence-display-name')?.textContent || '',focus:row.querySelector('.presence-focus').textContent,load:row.querySelector('.presence-scope').textContent}))");
    const all = await direct("presence.list", { project: fixture.project, _meta: meta() });
    assert.equal(all.error, undefined);
    assert.equal(all.result.presence.length, 50);
    assert.equal(new Set(all.result.presence.map((row) => row.sessionId)).size, 50);
    assert(all.result.presence.every((row) => row.workingOn.length === 1));
    assert.equal(presence.length, 5);
    assert.match(await text("#presence-rows"), /45 more/);
    outcome.observations.push({ visible: presence, text: await text("#presence-rows"), realPresence: all.result });
    const wireLabels = all.result.presence.map((row) => row.displayName).filter((label) => typeof label === "string" && label);
    const visibleLabels = presence.map((row) => row.label.replace(/^Client label \(untrusted\):\s*/, "")).filter(Boolean);
    if (wireLabels.length !== 50 || new Set(wireLabels).size !== 50) outcome.missingFields.push("presence.list does not return 50 distinct displayName values for the simulated agents");
    if (visibleLabels.length !== 5 || new Set(visibleLabels).size !== 5) outcome.missingFields.push("displayName consumer is not exposed as five distinct visible Client label (untrusted) rows");
  });

  await task(4, ["persisted parent relation", "authorized epic child membership", "done/total/active/blocked/ready counts", "bounded scope/trend signal", "epic browser surface"], async (outcome) => {
    outcome.startedAt = performance.now();
    const navigation = await evaluate("Array.from(document.querySelectorAll('[data-view]')).map(x=>({view:x.dataset.view,text:x.textContent}))");
    const result = await direct("view.run", { definition: { filter: { resources: [fixture.blockedId] }, fields: [], groupBy: ["parent"], budgetTokens: 25 }, _meta: meta() });
    outcome.observations.push({ navigation, actualRPCRefusal: calls.at(-1) });
    assert(result.error, "Epic parent grouping unexpectedly became available; update the exact task instead of keeping a stale block");
    outcome.missingFields.push("pipeline.Document has no parent relation; Create(parent)/Reparent refuse; viewops supports groupBy type only, no parent/assignee projection; no epic UI path");
  });

  // Qualification control: prove a CURRENT stored denial revokes the saved
  // view's reader. Never substitute a permit callback or fake watch success.
  await select("home");
  await until(async () => /connected/i.test(await text("#watch-state")), "real watch for stored revocation");
  const revoked = { ...fixture.grant, resources: fixture.grant.resources.filter((id) => id !== fixture.homeViewId) };
  const denial = await direct("tx.apply", { changeSet: { ops: [{ kind: "setMeta", target: { id: fixture.grantCarrierId, branch: 0 }, key: fixture.grantKey, value: revoked }] }, _meta: meta(fixture.revokeKey) });
  assert.equal(denial.error, undefined, JSON.stringify(denial));
  await until(async () => (await domRows("#changed-rows .row")).length === 0 && !/connected/i.test(await text("#watch-state")), "stored grant revocation removes stale browser rows");
  receipt.infrastructure.storedRevocationDOM = await text("#changed-rows");
  await send("Page.navigate", { url: "about:blank" });
  receipt.infrastructure.cleanup = await until(async () => {
    const stats = await (await fetch(base + "/fixture/e3/stats")).json();
    return stats.sessions === 50 && stats.streams === 0 && stats.watches === 0 && stats.policyAllows > 0 && stats.policyDenials > 0 ? stats : false;
  }, "actual session/watch/stream cleanup");
  await Promise.allSettled([...pendingBodies]);
  const productMethods = browserWire.filter((entry) => entry.request.url.endsWith("/rpc/v1")).map((entry) => JSON.parse(entry.request.postData || "{}").method);
  assert(productMethods.includes("view.run") && productMethods.includes("view.watch"));
  assert(productMethods.every((method) => ["server.hello", "rpc.methods", "session.open", "session.close", "view.run", "view.explain", "view.watch", "event.unsubscribe", "bead.ready", "presence.list"].includes(method)), productMethods.join(","));
  for (const entry of browserWire) {
    const headers = Object.keys(entry.request.headers).map((name) => name.toLowerCase());
    assert(!headers.includes("authorization") && !headers.includes("cookie"));
  }
  assert.equal(consoleErrors.length, 0, JSON.stringify(consoleErrors));
  assert.equal(egress.length, 0, JSON.stringify(egress));
  receipt.infrastructure.productMethods = productMethods;
  receipt.infrastructure.complete = true;
} catch (error) {
  receipt.infrastructure.error = String(error.stack || error);
  process.exitCode = 1;
} finally {
  for (const item of tasks) if (item.elapsedMs === null) { item.status = "NOT_RUN"; item.elapsedMs = 0; item.observations.push("Recorder stopped before this task; no qualification claimed."); }
  await writeFile(path.join(evidence, "browser-outcomes.json"), JSON.stringify(receipt, null, 2));
  await writeFile(path.join(evidence, "browser-wire.json"), JSON.stringify(browserWire, null, 2));
  await writeFile(path.join(evidence, "harness-rpc.json"), JSON.stringify(calls, null, 2));
  await writeFile(path.join(evidence, "browser-console.json"), JSON.stringify(consoleErrors, null, 2));
  await writeFile(path.join(evidence, "browser-egress.json"), JSON.stringify(egress, null, 2));
  if (ws) ws.close();
  const exited = once(chrome, "exit"); chrome.kill("SIGTERM");
  await Promise.race([exited, delay(3000)]);
  await rm(profile, { recursive: true, force: true }).catch(() => {});
  if (receipt.infrastructure.error) console.error(receipt.infrastructure.error);
}
}

// Importing the real predicate for focused checks must not start the workload.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await runBrowser();
