import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const config = JSON.parse(Buffer.concat(chunks).toString());
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-cors-chrome-"));
const chrome = spawn(process.argv[2], ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws;
chrome.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, description) {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) { if (await fn()) return; await delay(40); }
  throw new Error("Timed out: " + description);
}
try {
  await until(() => stderr.includes("DevTools listening on "), "Chrome startup");
  const debug = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1];
  const target = await (await fetch("http://" + new URL(debug).host + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await once(ws, "open");
  let sequence = 0;
  const pending = new Map(), responses = [], methods = [];
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error("Chrome command failed")) : promise.resolve(event.result);
    } else if (event.method === "Network.responseReceived" && event.params.response.url === config.rpc) {
      responses.push(event.params.response);
    } else if (event.method === "Network.requestWillBeSent" && event.params.request.url === config.rpc) {
      methods.push(event.params.request.method);
    }
  });
  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error("Browser fixture evaluation failed");
    return result.result.value;
  };
  await send("Page.enable"); await send("Network.enable");
  await send("Security.setIgnoreCertificateErrors", { ignore: true });
  const navigate = async (url) => {
    await send("Page.navigate", { url });
    await until(() => evaluate(`location.origin === ${JSON.stringify(url)} && document.readyState === "complete"`), "client navigation");
  };
  const call = (credentials, token) => evaluate(`(async () => {
    try {
      const response = await fetch(${JSON.stringify(config.rpc)}, {
        method:"POST", mode:"cors", credentials:${JSON.stringify(credentials)},
        headers:{"Authorization":${JSON.stringify("Bearer " + token)},"Content-Type":"application/json","Plug-RPC":"1"},
        body:JSON.stringify({jsonrpc:"2.0",id:"cors-safety",method:"rpc.methods",params:{}}),
      });
      const value = await response.json();
      return {status:response.status,id:value.id,methods:Array.isArray(value.result?.methods),code:value.error?.data?.code};
    } catch (_) { return {blocked:true}; }
  })()`);

  await navigate(config.allowed);
  const permitted = await call("omit", config.token);
  assert.equal(permitted.status, 200); assert.equal(permitted.id, "cors-safety"); assert.equal(permitted.methods, true);
  assert(methods.includes("OPTIONS"), "browser did not execute a real preflight");
  const headers = responses.filter((response) => response.status === 200).map((response) => Object.fromEntries(Object.entries(response.headers).map(([key, value]) => [key.toLowerCase(), value])));
  assert(headers.length > 0);
  for (const header of headers) {
    assert.equal(header["access-control-allow-origin"], config.allowed);
    assert.equal(header["access-control-allow-credentials"], undefined);
    assert.equal(header["set-cookie"], undefined);
  }
  console.log("PASS actual_browser_preflight_and_bound_bearer_without_CORS_credentials");

  const untrusted = await call("omit", "invalid-test-only");
  assert.equal(untrusted.status, 403); assert.equal(untrusted.methods, false);
  const cookie = await send("Network.setCookie", { name:"i7-cors-canary", value:"test-only", url:config.rpc, secure:true, sameSite:"None" });
  assert.equal(cookie.success, true);
  assert.equal((await call("include", config.token)).blocked, true);
  console.log("PASS CORS_credentials_include_and_untrusted_bearer_refused");

  await navigate(config.other);
  assert.equal((await call("omit", config.token)).blocked, true);
  console.log("PASS unlisted_browser_origin_cannot_read_RPC");
} finally {
  if (ws) ws.close();
  chrome.kill("SIGTERM");
  await Promise.race([once(chrome, "exit"), delay(2000)]);
  if (chrome.exitCode === null) { chrome.kill("SIGKILL"); await once(chrome, "exit"); }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
