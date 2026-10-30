// Consumer-only canonical and malformed response fixtures. This does not
// qualify collab normalization, authentication, expiry, or the E3 workload.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, chromePath, casesPath] = process.argv.slice(2);
const fixtures = JSON.parse(await readFile(casesPath, "utf8"));
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-displayname-contract-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--disable-sync", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "";
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description) {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) { if (await fn()) return; await delay(25); }
  throw new Error("Timed out: " + description);
}
let ws;
try {
  await until(() => /DevTools listening on ws:\/\/[^\s]+/.test(stderr), "browser startup");
  const debug = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1];
  const target = await (await fetch("http://" + new URL(debug).host + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map(), requests = [], errors = [];
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error("browser command refused")) : promise.resolve(event.result);
    } else if (event.method === "Network.requestWillBeSent" && event.params.request.url.includes("/rpc")) {
      requests.push(JSON.parse(event.params.request.postData));
    } else if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: false });
  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    assert(!result.exceptionDetails, "browser assertion expression failed");
    return result.result.value;
  };
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "presence read completion");
  const rendered = () => evaluate(`({
    state: document.querySelector('#connection-state').textContent,
    overflow: document.documentElement.scrollWidth > innerWidth,
    text: document.querySelector('#presence-rows').textContent,
    rows: Array.from(document.querySelectorAll('#presence-rows .presence-row'), row => ({
      name: row.querySelector('.presence-name')?.textContent,
      identity: row.querySelector('.presence-identity')?.textContent,
      label: row.querySelector('.presence-display-name')?.textContent || '',
      focus: row.querySelector('.presence-focus')?.textContent,
      scope: row.querySelector('.presence-scope')?.textContent,
      elements: row.querySelector('.presence-display-name')?.children.length || 0,
    })),
    remainder: document.querySelector('#presence-rows .elision')?.textContent || '',
    injected: !!document.querySelector('#presence-rows img, #presence-rows script') || globalThis.labelExecuted === true,
  })`);
  for (const fixture of fixtures) {
    const url = base + "/fixtures/" + fixture.name + "/ui/?view=presence";
    await send("Page.navigate", { url });
    await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "fixture document");
    await settled();
    const result = await rendered();
    assert.equal(result.injected, false, fixture.name + " must use text rendering");
    assert.equal(result.overflow, false, fixture.name + " must stay within the mobile viewport");
    if (fixture.refused) {
      assert.equal(result.state, "Unavailable", fixture.name + " must refuse malformed output");
      assert.match(result.text, /invalid_response/);
      assert.equal(result.rows.length, 0);
    } else {
      assert.equal(result.state, "Connected", fixture.name);
      assert.deepEqual(result.rows.map(row => row.name), fixture.names || []);
      assert.deepEqual(result.rows.map(row => row.identity), fixture.identities || []);
      assert.deepEqual(result.rows.map(row => row.label), (fixture.labels || []).map(label => label ? "Client label (untrusted): " + label : ""));
      for (const row of result.rows) {
        assert.equal(row.elements, 0, "caller label must never become markup");
        assert.equal(row.focus, "Focus bead/pl-focus · Working on bead/pl-a, bead/pl-b, bead/pl-c + 1 more");
        assert.equal(row.scope, "4 working");
      }
      for (const member of fixture.presence) assert(!result.text.includes(member.sessionId), "session membership token must not become displayed identity");
      if (fixture.name === "compact") {
        assert.equal(result.rows.length, 5);
        assert.equal(result.remainder, "2 more visible sessions are hidden to keep this view compact.");
        assert(!result.text.includes("worker 6") && !result.text.includes("worker 7"));
      }
      if (fixture.name === "empty") assert.match(result.text, /No active sessions/);
      if (fixture.refresh) {
        await evaluate("document.querySelector('#refresh').click()"); await settled();
        const refreshed = await rendered();
        assert.equal(refreshed.rows.length, 0, "stale labels must clear on the next empty/refused read");
        assert.equal(refreshed.state, fixture.refresh === "empty" ? "Connected" : "Unavailable");
        assert.match(refreshed.text, fixture.refresh === "empty" ? /No active sessions/ : /forbidden/);
      }
    }
    console.log("PASS display_name_consumer_contract_fixture=" + fixture.name);
  }
  assert.equal(errors.length, 0, "browser fixture must have no uncaught exceptions");
  assert(requests.length >= fixtures.length * 3);
  for (const request of requests) {
    assert(["server.hello", "rpc.methods", "presence.list"].includes(request.method), "consumer fixtures must remain read only");
    assert(!Object.hasOwn(request.params, "displayName"), "returned labels must never become request authority");
    if (request.method !== "server.hello") assert.deepEqual(request.params._meta, { project: "web" });
  }
  console.log("PASS display_name_consumer_contract_fixtures=" + fixtures.length + " skipped=0 qualification=fixture_only");
} finally {
  if (ws) ws.close();
  if (chrome.exitCode === null) {
    const exited = once(chrome, "exit"); chrome.kill("SIGTERM"); await exited;
  }
  await rm(profile, { recursive: true, force: true });
}
