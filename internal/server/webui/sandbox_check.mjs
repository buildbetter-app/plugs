// Runs only against the Go test's disposable loopback UI/RPC and egress trap.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, trap, chromePath] = process.argv.slice(2);
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-sandbox-chrome-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws;
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description) {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) { try { const value = await fn(); if (value) return value; } catch (_) {} await delay(40); }
  throw new Error("Timed out: " + description);
}
try {
  const debug = await until(() => stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1], "Chrome startup");
  const target = await (await fetch("http://" + new URL(debug).host + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map(), frameSessions = new Map();
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params, sessionId }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error(JSON.stringify(event.error))) : promise.resolve(event.result);
    } else if (event.method === "Target.attachedToTarget" && event.params.targetInfo.type === "iframe") {
      frameSessions.set(event.params.targetInfo.targetId, event.params.sessionId);
    }
  });
  const evaluate = async (expression, sessionId) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId);
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Page.navigate", { url: base + "/ui/" });
  const frameSession = async (id) => until(async () => {
    const { targetInfos } = await send("Target.getTargets");
    const frame = targetInfos.find((info) => info.type === "iframe" && info.url === base + "/ui/frames/" + id + "/index.html");
    if (!frame) return;
    if (!frameSessions.has(frame.targetId)) frameSessions.set(frame.targetId, (await send("Target.attachToTarget", { targetId: frame.targetId, flatten: true })).sessionId);
    return frameSessions.get(frame.targetId);
  }, id + " frame");
  let probe = await frameSession("probe");
  const sibling = await frameSession("sibling");
  await until(() => evaluate("globalThis.safetyLoaded === true && globalThis.safetyModuleLoaded === true", probe), "actual bundle classic/module script execution");
  assert.equal(await evaluate("document.querySelector('iframe').contentDocument.querySelector('iframe').getAttribute('sandbox')"), "allow-scripts");
  console.log("PASS actual_bundle_script_loads_under_product_headers");

  const runtimeProof = async (sessionId, embedded) => {
    const result = await evaluate(`(async () => {
      const denied = (fn) => { try { fn(); return false; } catch (error) { return error.name === "SecurityError"; } };
      const proof = {
        cookie: denied(() => document.cookie),
        localStorage: denied(() => localStorage.length),
        sessionStorage: denied(() => sessionStorage.length),
        parentDOM: ${embedded} ? denied(() => parent.document.body) : parent === window,
      };
      try { await fetch(${JSON.stringify(trap + "/fetch")}, {credentials:"include"}); proof.fetch = false; } catch (_) { proof.fetch = true; }
      proof.popup = window.open(${JSON.stringify(trap + "/popup")}) === null;
      proof.script = await new Promise((resolve) => {
        const script = document.createElement("script");
        script.onload = () => resolve(false); script.onerror = () => resolve(true);
        script.src = ${JSON.stringify(base + "/outside.js")}; document.body.appendChild(script);
      });
      proof.outsideCode = globalThis.outsideBundleExecuted === undefined;
      proof.image = await new Promise((resolve) => {
        const image = new Image(); image.onload = () => resolve(false); image.onerror = () => resolve(true);
        image.src = ${JSON.stringify(trap + "/image")}; document.body.appendChild(image);
      });
      const form = document.createElement("form"); form.action = ${JSON.stringify(trap + "/form")}; form.method = "POST"; document.body.appendChild(form); form.submit();
      return proof;
    })()`, sessionId);
    for (const [capability, denied] of Object.entries(result)) assert.equal(denied, true, `${embedded ? "frame" : "direct"} ${capability}`);
    console.log("PASS " + (embedded ? "frame" : "direct_open") + "_opaque_storage_network_and_own_path_CSP");
  };
  await runtimeProof(probe, true);

  // A real cookie exists in the browser before the bridge fetch. Its omission
  // must come from product fetch credentials mode, not an empty cookie jar.
  const cookie = await send("Network.setCookie", { name: "i7-canary", value: "test-only", url: base + "/", sameSite: "Lax" });
  assert.equal(cookie.success, true);
  const call = (sessionId, id, method) => evaluate(`new Promise((resolve) => {
    const timer = setTimeout(() => resolve({timeout:true}), 5000);
    const listener = (event) => { if (event.source === parent && event.data?.id === ${JSON.stringify(id)}) { clearTimeout(timer); removeEventListener("message", listener); resolve(event.data); } };
    addEventListener("message", listener);
    parent.postMessage({jsonrpc:"2.0",id:${JSON.stringify(id)},method:${JSON.stringify(method)},params:{}}, "*");
  })`, sessionId);
  const permitted = await call(probe, "permitted", "rpc.methods");
  assert.equal(permitted.error, undefined); assert(Array.isArray(permitted.result?.methods));
  const refused = await call(sibling, "sibling", "rpc.methods");
  assert.equal(refused.error?.data?.code, "method_not_granted");
  const d16 = await call(probe, "d16", "plugin.apply");
  assert.equal(d16.error?.data?.code, "method_not_granted");
  console.log("PASS event_source_bound_bridge_omits_real_cookie_and_denies_sibling_D16");

  // Both destinations are controlled fixtures. The trusted wrapper's policy
  // must contain navigation before another document can execute or inherit a
  // bridge, including same-origin sibling bundles with a different grant.
  const stats = async () => (await fetch(base + "/safety-stats")).json();
  for (const destination of [base + "/ui/frames/sibling/index.html", "data:text/html,<title>Local navigation fixture</title>"]) {
    await until(() => evaluate("document.querySelector('#cancel')?.hidden === true"), "host read completion before navigation");
    const before = await stats();
    try { await evaluate(`location.assign(${JSON.stringify(destination)})`, probe); } catch (_) {}
    await until(() => evaluate("document.querySelector('iframe').dataset.bridgeState === 'revoked'"), "contained navigation revokes bridge");
    await delay(100);
    assert.deepEqual(await stats(), before, "navigation must not load a sibling document or reach RPC");
    console.log("PASS navigation_contained_before_document_or_bridge_transfer " + (destination.startsWith("data:") ? "local-data" : "sibling-bundle"));
    await send("Network.clearBrowserCookies");
    await send("Page.reload");
    probe = await frameSession("probe");
    await until(() => evaluate("globalThis.safetyLoaded === true && globalThis.safetyModuleLoaded === true", probe), "original bundle reload");
    const restoredCookie = await send("Network.setCookie", { name: "i7-canary", value: "test-only", url: base + "/", sameSite: "Lax" });
    assert.equal(restoredCookie.success, true);
    const restored = await call(probe, "after-reload", "rpc.methods");
    assert.equal(restored.error, undefined); assert(Array.isArray(restored.result?.methods));
    console.log("PASS original_bundle_bridge_restored_after_host_reload");
  }

  // Direct access with credentials fails before any plugin document is served.
  await send("Page.navigate", { url: base + "/ui/frames/probe/index.html" });
  await until(() => evaluate("document.body?.textContent.includes('credentials are not configured')"), "credentialed direct open refusal");
  await send("Network.clearBrowserCookies");
  await send("Page.navigate", { url: base + "/ui/frames/probe/index.html" });
  await until(() => evaluate("globalThis.safetyLoaded === true && globalThis.safetyModuleLoaded === true"), "direct bundle classic/module script execution");
  await runtimeProof(undefined, false);
  console.log("PASS direct_open_with_credentials_refused");
} finally {
  if (ws) ws.close();
  chrome.kill("SIGTERM");
  await Promise.race([once(chrome, "exit"), delay(2000)]);
  if (chrome.exitCode === null) { chrome.kill("SIGKILL"); await once(chrome, "exit"); }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
