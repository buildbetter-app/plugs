// Invoked only by the Bazel Go test, against its disposable real HTTP server.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, evidence, chromePath] = process.argv.slice(2);
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-webui-chrome-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "";
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description, timeout = 8000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await fn(); if (value) return value; await delay(40); }
  throw new Error("Timed out: " + description);
}
let ws;
try {
  const debug = await until(() => stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1], "Chrome DevTools startup");
  const address = "http://" + new URL(debug).host;
  const target = await (await fetch(address + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map();
  const events = [];
  const requests = new Map();
  const responseJobs = [];
  const errors = [];
  const frameSessions = new Map();
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params, sessionId }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error(JSON.stringify(event.error))) : promise.resolve(event.result);
      return;
    }
    if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
    if (event.method === "Runtime.consoleAPICalled" && event.params.type === "error") errors.push(event.params);
    if (event.method === "Log.entryAdded" && event.params.entry.level === "error") errors.push(event.params.entry);
    if (event.method === "Target.attachedToTarget" && event.params.targetInfo.type === "iframe") {
      frameSessions.set(event.params.targetInfo.targetId, event.params.sessionId);
    }
    if (event.method === "Network.requestWillBeSent" && event.params.request.url === base + "/rpc/v1") {
      const entry = { request: event.params.request }; requests.set(event.params.requestId, entry); events.push(entry);
    }
    if (event.method === "Network.responseReceived" && requests.has(event.params.requestId)) requests.get(event.params.requestId).response = event.params.response;
    if (event.method === "Network.loadingFinished" && requests.has(event.params.requestId)) {
      const entry = requests.get(event.params.requestId);
      responseJobs.push(send("Network.getResponseBody", { requestId: event.params.requestId }).then((body) => { entry.body = body.body; }).catch(() => {}));
    }
  });
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Page.enable"); await send("Runtime.enable"); await send("Log.enable"); await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1100, deviceScaleFactor: 1, mobile: false });
  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  const value = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ""`);
  const click = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const settled = () => until(async () => await value("#connection-state") !== "Loading" && await evaluate("document.querySelector('#cancel').hidden"), "bounded read completion");
  const navigate = async (url) => { await send("Page.navigate", { url }); await until(async () => await evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "document load"); await settled(); };
  const screenshot = async (name) => {
    const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
    await writeFile(path.join(evidence, name + ".png"), Buffer.from(png.data, "base64"));
  };
  const proof = (name, detail = "") => console.log("PASS " + name + (detail ? " " + detail : ""));

  await navigate(base + "/ui/");
  assert.equal(await value("#connection-state"), "Connected");
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"), 2);
  assert.equal(await value("#revision-count"), "3");
  // Chrome runs opaque sandbox frames in separate processes. Execute only the
  // test sender in their default contexts; keep every product CSP/header and
  // the browser's real event.source/origin/structured-clone behavior intact.
  const frameSession = async (name) => until(async () => {
    const { targetInfos } = await send("Target.getTargets");
    const target = targetInfos.find((info) => info.type === "iframe" && info.url === base + "/ui/frames/" + name + "/index.html");
    return target && frameSessions.get(target.targetId);
  }, name + " frame session");
  const scopeSession = await frameSession("scope-probe");
  const siblingSession = await frameSession("sibling-probe");
  const frameRPCStart = events.length;
  const frameEvaluate = async (sessionId, expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId);
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  const scopeResponses = await frameEvaluate(scopeSession, `(async () => {
    const requests = [
      { id: "scope-top", params: {project: "other"} },
      { id: "scope-nested", params: {selector: {project: "other"}} },
      { id: "scope-ops", params: {ops: [{kind: "create", project: "other"}]} },
      { id: "scope-projects", params: {projects: ["web", "other"]} },
      { id: "scope-deep-array", params: {ops: [[{project: "other"}]]} },
      { id: "scope-null-project", params: {project: null} },
      { id: "scope-number-project", params: {project: 7} },
      { id: "scope-empty-project", params: {project: ""} },
      { id: "scope-meta", params: {_meta: {project: "other"}} },
      { id: "scope-null-list", params: {projects: null} },
      { id: "scope-bad-list", params: {projects: "web"} },
      { id: "scope-sparse-list", params: {projects: Array(1)} },
      { id: "scope-safe", params: {limit: 1, _meta: {project: "web"}} },
      { id: "scope-default", params: {limit: 1} },
    ];
    const responses = [];
    const done = new Promise((resolve) => window.addEventListener("message", (event) => {
      if (event.source === window.parent && event.data?.jsonrpc === "2.0" && requests.some((request) => request.id === event.data.id)) {
        responses.push(event.data);
        if (responses.length === requests.length) resolve(responses);
      }
    }));
    requests.forEach((request) => window.parent.postMessage({jsonrpc: "2.0", method: "bead.ready", ...request}, "*"));
    return done;
  })()`);
  for (const response of scopeResponses) {
    if (response.id === "scope-safe" || response.id === "scope-default") {
      assert.equal(response.error, undefined, JSON.stringify(response));
      assert.equal(response.result.items.length, 1);
    } else assert.equal(response.error?.data?.code, "scope_denied", JSON.stringify(response));
  }
  const siblingResponse = await frameEvaluate(siblingSession, `new Promise((resolve) => {
    window.addEventListener("message", (event) => { if (event.source === window.parent && event.data?.id === "sibling-call") resolve(event.data); });
    window.parent.postMessage({jsonrpc: "2.0", id: "sibling-call", method: "bead.ready", params: {project: "web", limit: 1}}, "*");
  })`);
  assert.equal(siblingResponse.error?.data?.code, "method_not_granted", JSON.stringify(siblingResponse));
  const frameRequests = events.slice(frameRPCStart).map((entry) => JSON.parse(entry.request.postData));
  assert.equal(frameRequests.length, 2, JSON.stringify(frameRequests));
  for (const request of frameRequests) {
    assert.equal(request.method, "bead.ready");
    assert.equal(request.params.project, undefined);
    assert.equal(request.params._meta.project, "web");
  }
  await writeFile(path.join(evidence, "bridge-scope.json"), JSON.stringify({scopeResponses, siblingResponse, frameRequests}, null, 2));
  proof("browser_bridge_project_scope", "top-level/nested/operation/list project escapes refused");
  proof("browser_bridge_sibling_identity", "sibling frame cannot borrow the granted frame identity");
  const initialRows = await evaluate("Array.from(document.querySelectorAll('#changed-rows .row-title'), x => x.textContent)");
  assert.equal(await evaluate("document.querySelector('#changed-more').hidden"), false);
  assert.match(await value("#changed-more"), /count is not supplied/);
  await screenshot("home-desktop");
  proof("stored_rows", JSON.stringify(initialRows));

  await click("#changed-more button"); await settled();
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"), 1);
  const finalRow = await value("#changed-rows .row-title");
  const titles = initialRows.concat([finalRow]);
  assert.deepEqual(new Set(titles), new Set(["Review the stored change", "<img src=x onerror=alert(1)>", "An unread page stays unread"]));
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows img').length"), 0);
  assert.equal(await value("#changed-count"), "1");
  proof("bounded_continuation_and_unread_rows", "2 then 1 stored rows; no append or unseen watermark");
  proof("untrusted_title_is_text");

  await click("#refresh"); await settled();
  assert.match(await value("#changed-rows"), /Nothing changed in this page/);
  assert.equal(await value("#changed-count"), "0");
  proof("unchanged_page_empty_state");

  // The harness acts as a separate writer through the same real RPC endpoint.
  // No browser request writes, and the subsequent refresh must notice rev 4.
  const seeds = JSON.parse(await readFile(path.join(evidence, "seed-rpc.json"), "utf8"));
  const visibleSeed = seeds.find((seed) => seed.params?.fields?.title === initialRows[0]);
  assert(visibleSeed, "the first bounded page must identify one seeded row");
  const updateRequest = { jsonrpc: "2.0", id: "fixture-update", method: "bead.update", params: { id: visibleSeed.response.result.results[0].ref, fields: { title: "A stored update since the last look" }, _meta: { project: "web", idempotencyKey: "4".repeat(64) } } };
  const updateResponse = await (await fetch(base + "/rpc/v1", { method: "POST", headers: { "Content-Type": "application/json", "Plug-RPC": "1" }, body: JSON.stringify(updateRequest) })).json();
  assert.equal(updateResponse.error, undefined, JSON.stringify(updateResponse));
  assert.equal(updateResponse.result.revision, 4);
  await writeFile(path.join(evidence, "fixture-update-rpc.json"), JSON.stringify({ request: updateRequest, response: updateResponse }, null, 2));
  await click("#refresh"); await settled();
  assert.equal(await value("#changed-count"), "1");
  assert.equal(await value("#changed-rows .row-title"), "A stored update since the last look");
  assert.equal(await value("#revision-count"), "4");
  await screenshot("changed-after-update"); proof("stored_update_detected_on_refresh");
  await click('[data-view="next"]'); await settled();
  assert.equal(await evaluate("document.querySelectorAll('#next-rows .row').length"), 2);
  await click("#next-more button"); await settled();
  assert.equal(await evaluate("document.querySelectorAll('#next-rows .row').length"), 1);
  proof("real_ready_queue_is_bounded");

  for (const [tab, node, expected] of [["blocked", "#blocked-state", /Blocked view unavailable/], ["explain", "#explain-state", /Explanation unavailable/], ["presence", "#presence-rows", /Presence unavailable/]]) {
    await click('[data-view="' + tab + '"]'); await settled(); assert.match(await value(node), expected); proof(tab + "_unavailable");
  }
  await navigate(base + "/empty/ui/");
  assert.equal(await value("#connection-state"), "Connected");
  assert.equal(await value("#changed-count"), "0");
  await click('[data-view="next"]'); await settled();
  assert.match(await value("#next-rows"), /ready queue is empty/);
  proof("actual_empty_project");

  await navigate(base + "/ui/?view=next");
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
  assert.equal(await evaluate("document.documentElement.scrollWidth <= innerWidth"), true);
  await screenshot("queue-mobile"); proof("mobile_no_horizontal_overflow");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1100, deviceScaleFactor: 1, mobile: false });
  await fetch(base + "/fixture/pause", { method: "POST" });
  await click("#refresh");
  await until(async () => (await (await fetch(base + "/fixture/state")).json()).active > 0, "actual paused driver read");
  assert.equal(await value("#connection-state"), "Loading");
  assert.match(await value("#next-rows"), /Loading/); await screenshot("loading"); proof("actual_loading_state");
  await click("#cancel"); await settled();
  assert.match(await value("#global-message"), /Request cancelled; nothing changed/);
  await until(async () => { const s = await (await fetch(base + "/fixture/state")).json(); return s.cancelled > 0 && s.active === 0; }, "cancelled real HTTP/driver context");
  await screenshot("cancelled"); proof("cancelled_real_read_released");
  await click("#refresh");
  await until(async () => await value("#connection-state") === "Timed out", "real read deadline", 20000);
  await settled();
  assert.match(await value("#global-message"), /timed out/);
  await until(async () => { const s = await (await fetch(base + "/fixture/state")).json(); return s.cancelled > 1 && s.active === 0; }, "timeout released real driver read");
  await screenshot("timed-out"); proof("real_read_deadline_released");
  await fetch(base + "/fixture/resume", { method: "POST" });
  await fetch(base + "/fixture/fence", { method: "POST" });
  await click("#refresh"); await settled();
  assert.equal(await value("#connection-state"), "Unavailable");
  assert.match(await value("#global-message"), /busy/); await screenshot("refused"); proof("actual_lease_loss_refusal");
  await delay(500);
  await Promise.all(responseJobs);
  const productMethods = events.map((x) => JSON.parse(x.request.postData).method);
  assert(productMethods.every((x) => ["server.hello", "rpc.methods", "bead.ready", "history.query"].includes(x)));
  assert.equal(errors.length, 0, JSON.stringify(errors));
  await writeFile(path.join(evidence, "browser-rpc.json"), JSON.stringify(events, null, 2));
  await writeFile(path.join(evidence, "browser-console.json"), JSON.stringify(errors, null, 2));
  proof("console_no_errors_after_500ms"); proof("browser_read_only_rpc", productMethods.join(","));
} finally {
  if (ws) ws.close();
  const exited = once(chrome, "exit"); chrome.kill("SIGTERM");
  await Promise.race([exited, delay(3000)]);
  await rm(profile, { recursive: true, force: true }).catch(() => {});
}
