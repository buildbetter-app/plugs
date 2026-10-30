// Consumer-only canonical and malformed response fixtures. This does not
// qualify collab normalization, authentication, expiry, or the E3 workload.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, chromePath, mode] = process.argv.slice(2);

const profile = await mkdtemp(path.join(os.tmpdir(), "plug-watch-bootstrap-"));
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
  const value = selector => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ""`);
  const serverState = async () => (await fetch(base + "/fixture/state")).json();
  const released = () => until(async () => {const s = await serverState(); return s.sessions === 0 && s.watches === 0 && s.streams === 0;}, "real lifecycle cleanup");
  const url = base + "/ui/";
  await send("Page.navigate", { url });
  await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "document");
  await until(async () => await value("#connection-state") === "Connected" && (await value("#watch-state")).includes("Live changes connected"), "stored ontology bootstrap and snapshot");
  await until(async () => (await value("#changed-rows")).includes("Intervening before reopen"), "real write between terminal frame and reopen");
  const opens = () => requests.filter(request => request.method === "view.watch");
  assert.equal(opens().length, 2); assert(!Object.hasOwn(opens()[0].params, "from"));
  const served = await (await fetch(base + "/fixture/delivered")).json();
  const terminal = served.flatMap(entry => entry.ndjson.trim().split("\n").filter(Boolean).map(line => JSON.parse(line))).find(frame => frame.events.some(event => event.type === "watch.reset"));
  assert(terminal); const reset = terminal.events.find(event => event.type === "watch.reset");
  assert.equal(terminal.events.length, 1); assert.equal(reset.details.reason, "ontology_changed");
  assert.equal(reset.details.snapshotCursor, terminal.cursor); assert.equal(reset.cursor, terminal.cursor); assert.equal(reset.details.subscriptionId, terminal.subscriptionId);
  assert.equal(opens()[1].params.from, terminal.cursor, "reopen must echo the delivered opaque server marker");
  const active = await serverState(); assert.equal(active.sessions, 1); assert.equal(active.watches, 1); assert.equal(active.streams, 1);
  const response = await (await fetch(base + "/rpc/v1", { method: "POST", headers: {"Content-Type":"application/json","Plug-RPC":"1"}, body: JSON.stringify({jsonrpc:"2.0", id:"actual-update",method:"bead.update",params:{id:{id:"pl-browser-a",branch:0},fields:{title:"Actual later update"},_meta:{project:"web",idempotencyKey:"a".repeat(64)}}})})).json();
  assert.equal(response.error, undefined, JSON.stringify(response));
  await until(async () => (await value("#changed-rows")).includes("Actual later update"), "real subsequent record delta");
  assert.equal(opens().length, 2);
  const path = mode === "ontology" ? "/fixture/ontology" : mode === "visibility" ? "/fixture/revoke" : "/fixture/reset";
  assert.equal((await fetch(base + path,{method:"POST"})).status, 204);
  await until(async () => await value("#connection-state") === "Unavailable", "later boundary remains fail closed");
  await released(); assert.equal(opens().length, 2, "later boundaries cannot automatically reopen");
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows .row').length"),0);
  assert((await value("#watch-state")).includes("refused"));
  const late = await (await fetch(base + "/fixture/delivered")).json();
  const frames = late.flatMap(entry => entry.ndjson.trim().split("\n").filter(Boolean).map(line => JSON.parse(line)));
  assert(frames.some(frame => frame.events.some(event => event.type === "watch.delta")));
  if (mode === "ontology") assert.equal(frames.filter(frame => frame.events.some(event => event.type === "watch.reset" && event.details.reason === "ontology_changed")).length, 2);
  if (mode === "visibility") assert(frames.some(frame => JSON.stringify(frame).includes("visibility.lost")));
  if (mode === "view") assert(frames.some(frame => frame.events.some(event => event.type === "watch.reset" && event.details.reason === "view_changed")));
  assert.equal(errors.length, 0);
  console.log("PASS real_stored_ontology_bootstrap mode=" + mode + " opaque_cursor_echo=true intervening_write=true later_delta=true later_boundary_refused=true leaked_sessions=0 leaked_watches=0 leaked_streams=0");
} finally {
  if (ws) ws.close();
  if (chrome.exitCode === null) { const exited = once(chrome, "exit"); chrome.kill("SIGTERM"); await exited; }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
