// Consumer-only canonical and malformed response fixtures. This does not
// qualify collab normalization, authentication, expiry, or the E3 workload.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, chromePath, casesPath] = process.argv.slice(2);

const profile = await mkdtemp(path.join(os.tmpdir(), "plug-comment-contract-"));
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
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "comment read completion");
  const rendered = () => evaluate(`({ state: document.querySelector('#connection-state').textContent, text: document.querySelector('#comment-state').textContent, actors: Array.from(document.querySelectorAll('.comment-actor'), n => n.textContent), sources: Array.from(document.querySelectorAll('.comment-source'), n => n.textContent), rows: document.querySelectorAll('.comment-row').length, injected: !!document.querySelector('#comment-state img, #comment-state script') || globalThis.commentExecuted === true, overflow: document.documentElement.scrollWidth > innerWidth })`);
  const cases = ["legacy", "import", "hostile", "no-author", "empty", "absent", "null", "empty-origin", "null-source", "null-author", "empty-author", "legacy-source", "control", "refresh"];
  for (const name of cases) {
    const url = base + "/fixtures/" + name + "/ui/?view=explain";
    await send("Page.navigate", { url });
    await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "fixture document");
    await settled();
    await evaluate("document.querySelector('#explain-id').value='pl-comments'; document.querySelector('#explain-form').requestSubmit()"); await settled();
    const value = await rendered(); assert.equal(value.injected, false, name); assert.equal(value.overflow, false, name);
    if (["null", "empty-origin", "null-source", "null-author", "empty-author", "legacy-source", "control"].includes(name)) {
      assert.equal(value.state, "Unavailable", name); assert.equal(value.rows, 0, name);
    } else {
      assert.equal(value.state, "Connected", name);
      if (["empty", "absent"].includes(name)) { assert.equal(value.rows, 0); assert.match(value.text, /No comments/); }
      else {
        assert.deepEqual(value.actors, ["Verified actor: user:restore-admin"]);
        if (name === "legacy") { assert.equal(value.sources.length, 0); assert.match(value.text, /old body/); }
        else { assert.match(value.text, /Restored at: 2026-10-30T10:00:00Z/); assert.match(value.sources[0], /Source time \(untrusted\): 2020-01-01T00:00:00Z/); }
        if (name === "hostile") assert(value.text.includes('<script>globalThis.commentExecuted=true</script>'));
        if (name === "no-author") assert.match(value.sources[0], /not supplied/);
        if (name === "refresh") { await evaluate("document.querySelector('#refresh').click()"); await settled(); const after = await rendered(); assert.equal(after.state, "Unavailable"); assert.equal(after.rows, 0); assert(!after.text.includes("secret-source")); }
      }
    }
    console.log("PASS comment_consumer_fixture=" + name);
  }
  assert.equal(errors.length, 0);
  const reads = requests.filter(request => request.method === "bead.get"); assert(reads.length >= cases.length);
  for (const request of reads) { assert.deepEqual(request.params.id, { id: "pl-comments", branch: 0 }); assert.deepEqual(request.params._meta, { project: "web" }); }
  assert(requests.every(request => ["server.hello", "rpc.methods", "bead.get"].includes(request.method)));
  console.log("PASS comment_consumer_fixtures=" + cases.length + " qualification=consumer_only");
} finally {
  if (ws) ws.close();
  if (chrome.exitCode === null) { const exited = once(chrome, "exit"); chrome.kill("SIGTERM"); await exited; }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
