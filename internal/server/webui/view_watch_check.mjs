// Run only against the Go test's disposable store/service/admitted HTTP host.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, evidence, chromePath] = process.argv.slice(2);
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-view-watch-chrome-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws;
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await fn(); if (value) return value; await delay(40); }
  throw new Error("Timed out: " + description);
}
const events = [], errors = [], writes = [], responseJobs = [];
let send;
try {
  const debug = await until(() => stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1], "Chrome startup");
  const address = "http://" + new URL(debug).host;
  const target = await (await fetch(address + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map(), requests = new Map();
  send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const task = pending.get(event.id); pending.delete(event.id);
      if (task) event.error ? task.reject(new Error(JSON.stringify(event.error))) : task.resolve(event.result);
      return;
    }
    if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
    if (event.method === "Runtime.consoleAPICalled" && event.params.type === "error") errors.push(event.params);
    if (event.method === "Network.requestWillBeSent" && [base + "/rpc/v1", base + "/watch/v1"].includes(event.params.request.url)) {
      const entry = { request: event.params.request }; requests.set(event.params.requestId, entry); events.push(entry);
    }
    if (event.method === "Network.responseReceived" && requests.has(event.params.requestId)) requests.get(event.params.requestId).response = event.params.response;
    if (event.method === "Network.loadingFinished" && requests.has(event.params.requestId)) {
      const entry = requests.get(event.params.requestId);
      responseJobs.push(send("Network.getResponseBody", { requestId: event.params.requestId }).then((body) => { entry.body = body.body; }).catch(() => {}));
    }
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1100, deviceScaleFactor: 1, mobile: false });
  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  const value = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ""`);
  const click = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const settled = () => until(async () => ["Connected", "Unavailable", "Cancelled", "Timed out"].includes(await value("#connection-state")) && await evaluate("document.querySelector('#cancel').hidden"), "bounded read");
  const navigate = async (suffix) => {
    const url = base + suffix;
    await send("Page.navigate", { url });
    await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "document load");
    await settled();
  };
  const live = () => until(async () => (await value("#watch-state")).includes("Live changes connected"), "real watch snapshot");
  const state = async () => (await fetch(base + "/fixture/state")).json();
  const released = () => until(async () => { const s = await state(); return s.sessions === 0 && s.watches === 0 && s.streams === 0; }, "session/subscription/stream cleanup");
  const titles = () => evaluate("Array.from(document.querySelectorAll('#changed-rows .row-title'), x => x.textContent)");
  const proof = (name, detail = "") => console.log("PASS " + name + (detail ? " " + detail : ""));
  const screenshot = async (name) => {
    const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
    await writeFile(path.join(evidence, name + ".png"), Buffer.from(png.data, "base64"));
  };
  let fixtureKey = 500;
  const update = async (id, title) => {
    const request = { jsonrpc: "2.0", id: "external-writer-" + (++fixtureKey), method: "bead.update", params: { id: { id }, fields: { title }, _meta: { project: "web", idempotencyKey: fixtureKey.toString(16).padStart(64, "0") } } };
    const response = await (await fetch(base + "/rpc/v1", { method: "POST", headers: { "Content-Type": "application/json", "Plug-RPC": "1" }, body: JSON.stringify(request) })).json();
    assert.equal(response.error, undefined, JSON.stringify(response)); writes.push({ request, response });
    return response.result.revision;
  };

  await navigate("/ui/"); await live();
  assert.deepEqual(await titles(), ["<img src=x onerror=alert(1)>", "Stored B"]);
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows img').length"), 0);
  assert.equal((await state()).sessions, 1);
  assert.equal((await state()).watches, 1);
  assert.equal((await state()).streams, 1);
  const snapshot = await value("#revision-count");
  await screenshot("stored-view-live");
  proof("admitted_saved_view_stored_rows", "2 bounded rows; text only; denied and foreign rows absent");

  const revision = await update("pl-browser-a", "A real live stored change");
  await until(async () => (await titles())[0] === "A real live stored change", "real delta reread without refresh");
  assert.equal(await value("#revision-count"), String(revision));
  assert.deepEqual(await titles(), ["A real live stored change", "Stored B"]);
  await screenshot("stored-live-delta");
  proof("admitted_watch_delta_rereads_view", "same page order, exact stored revision=" + revision);

  await click("#changed-more button"); await settled(); await released();
  assert.deepEqual(await titles(), ["Stored C"]);
  assert.equal(await value("#revision-count"), String(revision));
  await update("pl-browser-c", "C changed after the pinned page");
  assert.deepEqual(await titles(), ["Stored C"]);
  assert.match(await value("#watch-state"), /Snapshot page/);
  proof("pinned_view_continuation", "2 then 1; no duplicates, background watch/session released; later write excluded");

  await click("#refresh"); await settled(); await live();
  await fetch(base + "/fixture/reset", { method: "POST" });
  await until(async () => await value("#connection-state") === "Unavailable", "real saved-view membership reset");
  assert.match(await value("#watch-state"), /refused/);
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"), 0);
  assert.equal(await evaluate("document.querySelector('#changed-more').hidden"), true);
  assert.equal(await value("#revision-count"), "—");
  await released(); await screenshot("real-view-reset");
  proof("real_membership_reset_clears_rows_and_cursor");
  await fetch(base + "/fixture/restore", { method: "POST" });

  await click("#refresh"); await settled(); await live();
  await click("#stop-watch"); await released();
  assert.match(await value("#watch-state"), /cancelled/);
  assert.equal((await titles()).length, 2);
  proof("explicit_live_cancel_releases_owned_lifetime");
  await click("#refresh"); await settled(); await live();
  await click('[data-view="next"]'); await settled(); await released();
  assert.equal(await evaluate("document.querySelectorAll('#next-rows .row').length"), 2);
  proof("tab_change_cleans_view_watch_without_changing_ready_fixture");

  await navigate("/empty/ui/"); await live();
  assert.match(await value("#changed-rows"), /No stored rows in this view/);
  assert.equal(await value("#changed-count"), "0");
  await click("#stop-watch"); await released();
  proof("actual_empty_view_and_live_cancel");
  await fetch(base + "/fixture/hold-open", { method: "POST" });
  await send("Page.navigate", { url: base + "/slow/ui/" });
  await until(async () => (await state()).openingReads > 0, "actual opening saved-view read held");
  const cancelledAt = Date.now();
  await click("#cancel"); await settled();
  assert.equal(await value("#connection-state"), "Cancelled");
  assert(Date.now() - cancelledAt < 2500, "cancellation must not wait for the independent opening timeout");
  await fetch(base + "/fixture/release-open", { method: "POST" });
  await released();
  await until(async () => (await state()).openingReads === 0, "opening read released");
  proof("cancel_during_actual_watch_open_releases_late_result");
  await navigate("/ui/"); await live();
  await fetch(base + "/fixture/pause-view", { method: "POST" });
  await click("#refresh");
  await until(async () => (await state()).activeReads > 0, "actual view.run driver read held");
  await click("#cancel"); await settled(); await released();
  assert.equal(await value("#connection-state"), "Cancelled");
  await until(async () => { const s = await state(); return s.activeReads === 0 && s.cancelledReads > 0; }, "cancelled real view read released");
  await fetch(base + "/fixture/resume-view", { method: "POST" });
  proof("cancel_during_actual_view_read_releases_store_context_and_watch");
  for (const [suffix, expected] of [["/unsupported/ui/", /invalid_params/], ["/missing/ui/", /not_found/], ["/oversize/ui/", /response_exceeds_budget/]]) {
    await navigate(suffix); await released();
    assert.equal(await value("#connection-state"), "Unavailable");
    assert.match(await value("#global-message"), expected);
    assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"), 0);
    proof("real_refusal_" + suffix.split("/")[1], "no history/ready fallback or retained lifetime");
  }

  await navigate("/inline/ui/");
  assert.equal(await value("#connection-state"), "Connected");
  assert.equal((await titles()).length, 2);
  assert.match(await value("#watch-state"), /unavailable/);
  assert(!(await value("#changed-rows")).includes("DENIED"));
  proof("bounded_inline_view_run_without_saved_watch");
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
  assert.equal(await evaluate("document.documentElement.scrollWidth <= innerWidth"), true);
  await screenshot("view-mobile");
  proof("connected_view_mobile_no_overflow");

  await navigate("/ui/"); await live();
  await update("pl-browser-a", "Visible before actual revocation");
  await until(async () => (await titles())[0] === "Visible before actual revocation", "visible delivered resource before revocation");
  await fetch(base + "/fixture/revoke", { method: "POST" });
  await until(async () => await value("#connection-state") === "Unavailable", "real current stored read-grant revocation");
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"), 0);
  await released();
  proof("current_stored_permission_loss_removes_displayed_rows");
  await click("#refresh"); await settled(); await live();
  assert.deepEqual(await titles(), ["Stored B", "C changed after the pinned page"]);
  assert(!(await value("#changed-rows")).includes("Visible before actual revocation"));
  await send("Page.navigate", { url: "about:blank" }); await released();
  proof("pagehide_releases_session_stream_and_subscription");

  await delay(500); await Promise.all(responseJobs);
  const allowed = ["server.hello", "rpc.methods", "view.run", "view.watch", "event.unsubscribe", "session.open", "session.close", "bead.ready"];
  const methods = [];
  for (const entry of events) {
    const headers = Object.fromEntries(Object.entries(entry.request.headers).map(([key, value]) => [key.toLowerCase(), value]));
    assert.equal(headers.authorization, undefined); assert.equal(headers.cookie, undefined);
    assert.equal(new URL(entry.request.url).origin, base);
    if (entry.request.url.endsWith("/rpc/v1")) {
      const request = JSON.parse(entry.request.postData); methods.push(request.method);
      assert(allowed.includes(request.method), JSON.stringify(request));
      if (request.method !== "server.hello") assert.equal(request.params._meta.project, "web");
      if (["view.watch", "event.unsubscribe", "session.close"].includes(request.method)) assert(headers["plug-session"]);
    } else {
      assert(headers["plug-session"]); assert.equal(headers["plug-rpc"], "1");
      assert.equal(typeof JSON.parse(entry.request.postData).subscriptionId, "string");
    }
  }
  assert(methods.includes("view.run") && methods.includes("view.watch") && !methods.includes("history.query"));
  const continuations = events.filter((entry) => entry.request.url.endsWith("/rpc/v1")).map((entry) => JSON.parse(entry.request.postData)).filter((request) => request.method === "view.run" && request.params.cursor);
  assert.equal(continuations.length, 1);
  assert.equal(String(continuations[0].params.asOf.revision), String(revision));
  assert.notEqual(snapshot, "");
  assert.equal(errors.length, 0, JSON.stringify(errors));
  await writeFile(path.join(evidence, "browser-rpc-stream.json"), JSON.stringify(events, null, 2));
  await writeFile(path.join(evidence, "external-writes-rpc.json"), JSON.stringify(writes, null, 2));
  await writeFile(path.join(evidence, "final-store-state.json"), JSON.stringify(await state(), null, 2));
  proof("real_browser_wire_project_scope_and_read_only_product_calls", methods.join(","));
  proof("browser_console_no_errors_after_500ms");
} catch (error) {
  await writeFile(path.join(evidence, "failed-browser-wire.json"), JSON.stringify({ events, errors, writes }, null, 2));
  if (send) {
    const diagnostic = await send("Runtime.evaluate", { expression: "document.body?.innerText", returnByValue: true }).catch(() => ({}));
    console.error(JSON.stringify(diagnostic));
  }
  throw error;
} finally {
  if (ws) ws.close();
  const exited = once(chrome, "exit"); chrome.kill("SIGTERM");
  await Promise.race([exited, delay(3000)]);
  await rm(profile, { recursive: true, force: true }).catch(() => {});
}
