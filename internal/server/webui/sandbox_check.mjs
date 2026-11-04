// Runs only against the Go test's disposable loopback UI/RPC and egress trap.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, trap, chromePath, configuredJSON] = process.argv.slice(2);
const configured = configuredJSON ? JSON.parse(configuredJSON) : undefined;
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-sandbox-chrome-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws, observeBrowserFailure;
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
  const pending = new Map(), frameSessions = new Map(), bridgeResponses = [];
  // Failure observations contain fixture paths and CDP identities only. Do
  // not collect response bodies, cookies, headers, or plugin/RPC payloads.
  const recent = [];
  const remember = (event) => { recent.push(event); if (recent.length > 64) recent.shift(); };
  const fixturePath = (value) => {
    try { const url = new URL(value); return url.origin === new URL(base).origin ? url.pathname : url.protocol; }
    catch (_) { return "unavailable"; }
  };
  const targetState = (info) => ({ targetId: info.targetId, type: info.type, path: fixturePath(info.url), attached: info.attached });
  const frameState = (tree) => ({ frameId: tree.frame.id, parentId: tree.frame.parentId, loaderId: tree.frame.loaderId,
    path: fixturePath(tree.frame.url), children: (tree.childFrames || []).map(frameState) });
  let lastFrameLookup;
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject, method, sessionId }); ws.send(JSON.stringify({ id, method, params, sessionId }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise && event.error) remember({ kind: "protocol-error", method: promise.method, sessionId: promise.sessionId, code: event.error.code });
      if (promise) event.error ? promise.reject(new Error(JSON.stringify(event.error))) : promise.resolve(event.result);
    } else if (event.method === "Runtime.bindingCalled" && event.params.name === "__i7ObserveBridgeReply") {
      bridgeResponses.push(JSON.parse(event.params.payload));
    } else if (event.method === "Target.attachedToTarget" && event.params.targetInfo.type === "iframe") {
      frameSessions.set(event.params.targetInfo.targetId, event.params.sessionId);
      remember({ kind: event.method, sessionId: event.params.sessionId, target: targetState(event.params.targetInfo) });
    } else if (event.method === "Target.detachedFromTarget" || event.method === "Target.targetDestroyed" || event.method === "Page.frameDetached") {
      remember({ kind: event.method, sessionId: event.params.sessionId, targetId: event.params.targetId, frameId: event.params.frameId });
    } else if (event.method === "Page.frameNavigated") {
      remember({ kind: event.method, frame: frameState({ frame: event.params.frame }) });
    }
  });
  const evaluate = async (expression, sessionId) => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId);
    if (result.exceptionDetails) remember({ kind: "evaluation-exception", sessionId, exceptionId: result.exceptionDetails.exceptionId });
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  observeBrowserFailure = async () => {
    const bounded = (promise) => Promise.race([
      promise.then((value) => ({ value }), () => ({ unavailable: true })),
      delay(1000).then(() => ({ unavailable: true })),
    ]);
    const [targets, frames, host] = await Promise.all([
      bounded(send("Target.getTargets").then(({ targetInfos }) => targetInfos.map(targetState))),
      bounded(send("Page.getFrameTree").then(({ frameTree }) => frameState(frameTree))),
      bounded(evaluate(`({readyState:document.readyState, frames:Array.from(document.querySelectorAll('iframe'), (frame) => {
        const state = {path:new URL(frame.src).pathname, bridgeState:frame.dataset.bridgeState};
        try {
          const host = frame.contentDocument, child = host?.querySelector('iframe');
          return {...state, hostReadyState:host?.readyState, hostBridgeState:host?.documentElement.dataset.bridgeState,
            childPath:child ? new URL(child.src).pathname : null};
        } catch (_) { return {...state, hostAccessible:false}; }
      })})`)),
    ]);
    console.error("BROWSER_FAILURE_STATE " + JSON.stringify({ lastFrameLookup, cachedSessions: [...frameSessions], recent, targets, frames, host }));
  };
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Page.navigate", { url: base + "/ui/" });
  const frameSession = async (id) => until(async () => {
    lastFrameLookup = { id, state: "requesting targets" };
    const { targetInfos } = await send("Target.getTargets");
    lastFrameLookup = { id, targets: targetInfos.map(targetState) };
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
  assert((await send("Network.getCookies", { urls: [base + "/"] })).cookies.some((value) => value.name === "i7-canary" && value.value === "test-only"));
  const call = (sessionId, id, method, params = {}) => evaluate(`new Promise((resolve) => {
    const timer = setTimeout(() => resolve({timeout:true}), 5000);
    const listener = (event) => { if (event.source === parent && event.data?.id === ${JSON.stringify(id)}) { clearTimeout(timer); removeEventListener("message", listener); resolve(event.data); } };
    addEventListener("message", listener);
    parent.postMessage({jsonrpc:"2.0",id:${JSON.stringify(id)},method:${JSON.stringify(method)},params:${JSON.stringify(params)}}, "*");
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
  const localDocument = '<title>Local navigation fixture</title><script>parent.postMessage({jsonrpc:"2.0",id:"navigation-fixture",method:"rpc.methods",params:{}}, "*");</script>';
  for (const destination of [base + "/ui/frames/sibling/index.html#navigation-probe", "data:text/html," + encodeURIComponent(localDocument)]) {
    await until(() => evaluate("document.querySelector('#cancel')?.hidden === true"), "host read completion before navigation");
    const before = await stats();
    await evaluate(`(() => {
      globalThis.navigationPolicySeen = false;
      document.querySelector('iframe').contentWindow.addEventListener('securitypolicyviolation', (event) => {
        if (event.effectiveDirective === 'frame-src') globalThis.navigationPolicySeen = true;
      });
    })()`);
    try { await evaluate(`location.assign(${JSON.stringify(destination)})`, probe); } catch (_) {}
    await until(() => evaluate("document.querySelector('iframe').dataset.bridgeState === 'revoked'"), "contained navigation revokes bridge");
    await until(() => evaluate("globalThis.navigationPolicySeen === true"), "exact-entry CSP blocks navigation before the load fence");
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

  if (configured) {
    // These responses come from the actual configured RPC/SQLite store after
    // the source-built guest's Writer callbacks, not a browser fixture result.
    const read = await call(probe, "source-record", "bead.get", { id: configured.ref });
    assert.equal(read.error, undefined, JSON.stringify(read));
    assert.equal(read.result?.bead?.fields?.title, configured.title);
    assert.equal(read.result?.bead?.ref?.id || read.result?.bead?.id, configured.ref.id);
    console.log("PASS configured_source_Writer_record_read_through_actual_bridge");

    await until(() => evaluate("document.querySelector('#cancel')?.hidden === true"), "configured UI idle");
    const before = await stats();
    const siblingNow = await frameSession("sibling");
    await evaluate(`parent.parent.postMessage({jsonrpc:"2.0",id:"wrong-source",method:"bead.get",params:{id:${JSON.stringify(configured.ref)}}}, "*")`, siblingNow);
    await evaluate(`window.postMessage({jsonrpc:"2.0",id:"host-source",method:"bead.get",params:{id:${JSON.stringify(configured.ref)}}}, "*")`);
    await delay(150);
    assert.deepEqual(await stats(), before, "wrong WindowProxy must never reach configured RPC");
    for (const [id, method, params, code] of [
      ["write", "bead.update", {id:configured.ref,fields:{title:"hostile"}}, "method_not_granted"],
      ["cookie-field", "bead.get", {id:configured.ref,nested:{cookie:"test-canary"}}, "bridge_field_denied"],
      ["egress-field", "bead.get", {id:configured.ref,url:trap + "/bridge-egress"}, "bridge_field_denied"],
      ["foreign-project", "bead.get", {id:configured.ref,project:"foreign"}, "scope_denied"],
      ["forged-lifecycle", "plugin.apply", {plugFrameLifecycle:"revoked"}, "method_not_granted"],
      ["oversized", "bead.get", {id:configured.ref,padding:"x".repeat(300000)}, "message_too_large"],
    ]) {
      const refused = await call(probe, id, method, params);
      assert.equal(refused.error?.data?.code, code, JSON.stringify(refused));
    }
    assert.deepEqual(await stats(), before, "capability/source refusals must happen before RPC");
    console.log("PASS configured_wrong_source_capability_scope_credential_size_requests_never_reach_RPC");

    await delay(1100);
    const beforeRate = await stats();
    const rate = await evaluate(`new Promise((resolve, reject) => {
      const replies = [];
      const expected = new Set(Array.from({length: 24}, (_, i) => "rate-" + i));
      let timer;
      const cleanup = () => { clearTimeout(timer); removeEventListener("message", listener); };
      const listener = (event) => {
        if (event.source !== parent || !expected.has(event.data?.id)) return;
        expected.delete(event.data.id);
        replies.push(event.data);
        if (expected.size === 0) { cleanup(); resolve(replies); }
      };
      addEventListener("message", listener);
      for (let i = 0; i < 24; i++) parent.postMessage({jsonrpc:"2.0",id:"rate-"+i,method:"rpc.methods",params:{}}, "*");
      timer = setTimeout(() => {
        cleanup();
        reject(new Error("timed out waiting for rate replies: " + [...expected].join(",")));
      }, 30000);
    })`, probe);
    const rateLimited = rate.filter((reply) => reply.error?.data?.code === "rate_limited");
    const successful = rate.filter((reply) => Array.isArray(reply.result?.methods));
    const unclassified = rate.filter((reply) => reply.error?.data?.code !== "rate_limited" && !Array.isArray(reply.result?.methods));
    assert.equal(rate.length, 24);
    assert.equal(rateLimited.length, 4);
    assert.equal(successful.length, 20, JSON.stringify(rate));
    assert.equal(unclassified.length, 0, JSON.stringify(unclassified));
    assert.equal((await stats()).rpcCalls - beforeRate.rpcCalls, 20);
    console.log("PASS configured_bridge_rate_bound_20_actual_RPC_4_refusals");

    await delay(1100);
    // Observe where app.js sends actual responses: the trusted wrapper's
    // message receiver. CDP retains these real envelopes outside documents,
    // and installs the same observer before scripts in a reloaded wrapper.
    await send("Runtime.addBinding", { name: "__i7ObserveBridgeReply" });
    await send("Page.addScriptToEvaluateOnNewDocument", {
      runImmediately: true,
      source: `(() => {
        if (parent === window || location.href !== ${JSON.stringify(base + "/ui/frame-hosts/probe/index.html")}) return;
        addEventListener("message", (event) => {
          if (event.source === parent && event.origin === location.origin && event.data?.jsonrpc === "2.0" &&
              (Object.hasOwn(event.data, "result") || Object.hasOwn(event.data, "error"))) {
            globalThis.__i7ObserveBridgeReply(JSON.stringify(event.data));
          }
        });
      })()`,
    });
    const receiveControl = await call(probe, "receive-control", "bead.get", {id:configured.ref});
    assert.equal(receiveControl.result?.bead?.fields?.title, configured.title, JSON.stringify(receiveControl));
    await until(() => bridgeResponses.some((reply) => reply.id === "receive-control"), "real response reaches wrapper observer");
    assert.deepEqual(bridgeResponses.filter((reply) => reply.id === "receive-control"), [receiveControl]);
    console.log("PASS configured_wrapper_receive_observer_sees_actual_source_record_control");
    assert.equal((await fetch(base + "/fixture/hold", {method:"POST"})).ok, true);
    await evaluate(`parent.postMessage({jsonrpc:"2.0",id:"held",method:"bead.get",params:{id:${JSON.stringify(configured.ref)}}}, "*")`, probe);
    await until(async () => (await stats()).heldReads === 1, "real configured read response held");
    // The held bridge read already ran with the canary present. Ordinary
    // frame navigations carrying cookies are deliberately refused by the UI;
    // clear the canary to exercise the separate wrapper-reload boundary.
    await send("Network.clearBrowserCookies");
    await evaluate("document.querySelector('iframe').contentWindow.location.reload()");
    await until(() => evaluate("document.querySelector('iframe').dataset.bridgeState === 'revoked'"), "wrapper reload revokes bridge");
    await until(async () => (await stats()).canceledReads === 1, "reload aborts actual HTTP read");
    probe = await frameSession("probe");
    await until(() => evaluate("globalThis.safetyLoaded === true", probe), "reloaded child executes its pinned script");
    const afterRevoke = await stats();
    assert.deepEqual(await call(probe, "revoked", "bead.get", {id:configured.ref}), {timeout:true});
    assert.deepEqual(await stats(), afterRevoke, "reloaded wrapper must not regain the revoked grant");
    assert.deepEqual(bridgeResponses.filter((reply) => reply.id === "held"), [], "late response must not be transferred after revocation");
    console.log("PASS configured_wrapper_reload_aborts_HTTP_and_stale_bridge_stays_closed");
    await send("Network.clearBrowserCookies");
    const oldLoader = (await send("Page.getFrameTree")).frameTree.frame.loaderId;
    assert(oldLoader);
    await send("Page.reload");
    // Page.reload acknowledges before navigation commits. Wait for the real
    // new host document before attaching, so an old child target cannot be
    // mistaken for the new frame and then destroyed underneath the probe.
    await until(async () => (await send("Page.getFrameTree")).frameTree.frame.loaderId !== oldLoader &&
      await evaluate("document.readyState === 'complete' && document.querySelector('iframe') !== null"), "new host document after reload");
    probe = await frameSession("probe");
    await until(() => evaluate("globalThis.safetyLoaded === true", probe), "new host creates fresh bound frame");
    const recovered = await call(probe, "fresh-host", "bead.get", {id:configured.ref});
    assert.equal(recovered.result?.bead?.fields?.title, configured.title, JSON.stringify(recovered));
    console.log("PASS configured_fresh_host_bridge_reads_same_source_record_after_reload");
    await send("Network.setCookie", { name: "i7-canary", value: "test-only", url: base + "/", sameSite: "Lax" });
  }

  // Direct access with credentials fails before any plugin document is served.
  await send("Page.navigate", { url: base + "/ui/frames/probe/index.html" });
  await until(() => evaluate("document.body?.textContent.includes('credentials are not configured')"), "credentialed direct open refusal");
  await send("Network.clearBrowserCookies");
  await send("Page.navigate", { url: base + "/ui/frames/probe/index.html" });
  await until(() => evaluate("globalThis.safetyLoaded === true && globalThis.safetyModuleLoaded === true"), "direct bundle classic/module script execution");
  await runtimeProof(undefined, false);
  console.log("PASS direct_open_with_credentials_refused");

  if (configured) {
    await send("Page.navigate", { url: base + "/ui/" });
    probe = await frameSession("probe");
    await until(() => evaluate("globalThis.safetyLoaded === true", probe), "configured host before teardown");
    await until(() => evaluate("document.querySelector('#cancel')?.hidden === true"), "configured host read completion before teardown");
    const control = await call(probe, "before-close", "bead.get", {id:configured.ref});
    assert.equal(control.result?.bead?.fields?.title, configured.title, JSON.stringify(control));
    const closeResponse = await fetch(base + "/fixture/close", {method:"POST"});
    const closeBody = await closeResponse.text();
    assert.equal(closeResponse.ok, true, "configured Runtime.Close HTTP " + closeResponse.status + ": " + closeBody);
    const closed = await call(probe, "after-close", "bead.get", {id:configured.ref});
    assert(closed.error && !Object.hasOwn(closed, "result"), JSON.stringify(closed));
    assert.equal((await fetch(base + "/ui/config.json")).status, 502);
    console.log("PASS configured_Runtime_teardown_refuses_live_bridge_and_UI_access");
  }
} catch (error) {
  try { await observeBrowserFailure?.(); } catch (_) { console.error("BROWSER_FAILURE_STATE unavailable"); }
  throw error;
} finally {
  if (ws) ws.close();
  chrome.kill("SIGTERM");
  await Promise.race([once(chrome, "exit"), delay(2000)]);
  if (chrome.exitCode === null) { chrome.kill("SIGKILL"); await once(chrome, "exit"); }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
