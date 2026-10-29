// Supplemental browser evidence for the real shared-server fixture.
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";

const [base, evidence, chromePath] = process.argv.slice(2);
const fixture = JSON.parse(await readFile(path.join(evidence, "browser-fixture.json"), "utf8"));
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-web-acceptance-"));
const chrome = spawn(chromePath, [
  "--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
  "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0",
  "--user-data-dir=" + profile, "about:blank",
], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "";
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value;
    await delay(40);
  }
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
  const requests = [];
  const errors = [];
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id);
      pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error(JSON.stringify(event.error))) : promise.resolve(event.result);
      return;
    }
    if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
    if (event.method === "Runtime.consoleAPICalled" && event.params.type === "error") errors.push(event.params);
    if (event.method === "Log.entryAdded" && event.params.entry.level === "error") errors.push(event.params.entry);
    if (event.method === "Network.requestWillBeSent" && event.params.request.url === base + "/rpc/v1") requests.push(event.params.request);
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Log.enable");
  await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result?.value;
  };
  const value = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ""`);
  const click = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const settled = () => until(async () => (await value("#connection-state")) === "Connected" && await evaluate("document.querySelector('#cancel').hidden"), "connected read completion");
  const navigate = async (url) => {
    await send("Page.navigate", { url });
    await until(async () => await evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "document load");
    await settled();
  };
  const screenshot = async (name) => {
    const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
    await writeFile(path.join(evidence, name + ".png"), Buffer.from(png.data, "base64"));
  };
  const proof = (name, detail = "") => console.log("PASS " + name + (detail ? " " + detail : ""));

  await navigate(base + "/ui/");
  assert.equal(await value("#connection-state"), "Connected");
  assert.equal(await value("#changed-count"), "1");
  assert.equal(await value("#changed-rows .row-title"), "seed <unsafe> row");
  assert.equal(await evaluate("document.querySelectorAll('#changed-rows img').length"), 0);
  await screenshot("home");
  proof("real_rpc_row_rendered_as_text");

  const createRequest = {
    jsonrpc: "2.0", id: "fixture-create", method: "bead.create",
    params: { type: "issue", fields: { title: fixture.title }, _meta: { project: fixture.project, idempotencyKey: fixture.idempotencyKey } },
  };
  const createResponse = await (await fetch(base + "/rpc/v1", {
    method: "POST", headers: { "Content-Type": "application/json", "Plug-RPC": "1" }, body: JSON.stringify(createRequest),
  })).json();
  assert.equal(createResponse.error, undefined, JSON.stringify(createResponse));
  await writeFile(path.join(evidence, "refresh-create-rpc.json"), JSON.stringify({ request: createRequest, response: createResponse }, null, 2));
  await click("#refresh");
  await settled();
  assert.equal(await value("#changed-count"), "1");
  assert.equal(await value("#changed-rows .row-title"), fixture.title);
  await screenshot("refreshed");
  proof("rpc_row_appears_after_ui_refresh");

  await navigate(base + "/empty/ui/");
  await click('[data-view="next"]');
  await settled();
  assert.match(await value("#next-rows"), /ready queue is empty/);
  await screenshot("empty");
  proof("real_empty_state");

  await navigate(base + "/ui/");
  const fence = await fetch(base + "/fixture/fence", { method: "POST" });
  assert.equal(fence.status, 204);
  await click("#refresh");
  await until(async () => await value("#connection-state") === "Unavailable", "RPC refusal state");
  assert.match(await value("#global-message"), /busy/);
  assert.match(await value("#changed-rows"), /refused/);
  await screenshot("refused");
  proof("refused_connection_is_visible_in_dom");

  const productMethods = requests.map((request) => JSON.parse(request.postData || "{}").method);
  assert(productMethods.length > 0);
  assert(productMethods.every((method) => ["server.hello", "rpc.methods", "bead.ready", "history.query"].includes(method)), productMethods.join(","));
  assert.equal(errors.length, 0, JSON.stringify(errors));
  await writeFile(path.join(evidence, "browser-rpc.json"), JSON.stringify(requests, null, 2));
  await writeFile(path.join(evidence, "browser-console.json"), JSON.stringify(errors, null, 2));
  proof("browser_method_surface_is_read_only", productMethods.join(","));
  proof("browser_console_has_no_errors");
} finally {
  if (ws) ws.close();
  const exited = once(chrome, "exit");
  chrome.kill("SIGTERM");
  await Promise.race([exited, delay(3000)]);
  await rm(profile, { recursive: true, force: true }).catch(() => {});
}
