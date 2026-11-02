// A real Chrome page, not response interception or a synthetic RPC producer.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const receive = async () => JSON.parse((await lines.next()).value);
const fixture = await receive();
const emit = message => process.stdout.write(JSON.stringify(message) + "\n");
const control = async action => { emit({ event: "control", action }); return receive(); };
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-auth-epic-"));
const chrome = spawn(fixture.chrome, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--disable-sync", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { detached: true, stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws, send;
chrome.stderr.on("data", chunk => { stderr += chunk.toString(); });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, what) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) { if (await fn()) return; await delay(25); }
  throw new Error("Timed out: " + what);
}
const started = Date.now(), wire = [], consoleEntries = [], exceptions = [], steps = [];
const step = text => { steps.push({ atMs: Date.now() - started, text }); emit({ event: "step", text }); };
const identities = new Map([[fixture.token, fixture.principal], [fixture.narrowToken, "user:browser-child-only"], [fixture.restrictedToken, fixture.principal], [fixture.deniedToken, "agent:owner"]]);
const credentials = [fixture.token, fixture.narrowToken, fixture.restrictedToken, fixture.deniedToken].filter(Boolean);
let phase = "sign-in", base = fixture.base, probeSequence = 0;
try {
  await until(() => /DevTools listening on ws:\/\/[^\s]+/.test(stderr), "Chrome startup");
  const devtools = "http://" + new URL(stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1]).host;
  const target = await (await fetch(devtools + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl); await once(ws, "open");
  let sequence = 0; const pending = new Map(), network = new Map();
  send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString()), p = event.params;
    if (event.id) {
      const job = pending.get(event.id); pending.delete(event.id);
      if (job) event.error ? job.reject(new Error("Chrome command refused")) : job.resolve(event.result);
    } else if (event.method === "Runtime.exceptionThrown") exceptions.push(p.exceptionDetails);
    else if (event.method === "Runtime.consoleAPICalled") consoleEntries.push({ type: p.type, args: p.args.map(arg => arg.value ?? arg.description), atMs: Date.now() - started });
    else if (event.method === "Log.entryAdded") consoleEntries.push({ type: "browser-log", entry: p.entry });
    else if (event.method === "Network.requestWillBeSent" && p.request.url.endsWith("/rpc/v1")) {
      const headers = Object.fromEntries(Object.entries(p.request.headers).map(([key, value]) => [key.toLowerCase(), value]));
      const token = String(headers.authorization || "").replace(/^Bearer /, "");
      const row = { phase, atMs: Date.now() - started, url: p.request.url, request: JSON.parse(p.request.postData), authorizationPresent: !!headers.authorization, cookiePresent: !!headers.cookie, principal: identities.get(token) || "" };
      wire.push(row); network.set(p.requestId, row);
    } else if (event.method === "Network.responseReceived" && network.has(p.requestId)) {
      const row = network.get(p.requestId); row.status = p.response.status;
      if (row.status === 401) row.principal = "";
    } else if (event.method === "Network.loadingFinished" && network.has(p.requestId)) {
      const row = network.get(p.requestId);
      void send("Network.getResponseBody", { requestId: p.requestId }).then(body => {
        row.rawResponse = body.base64Encoded ? Buffer.from(body.body, "base64").toString() : body.body;
      }).catch(() => { exceptions.push("failed to collect actual Chrome response body"); });
    }
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable"); await send("Log.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  const evaluate = async expression => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    assert(!result.exceptionDetails, "browser evaluation failed"); return result.result.value;
  };
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "completed UI read");
  const completeWire = () => until(() => wire.every(row => typeof row.rawResponse === "string"), "actual response bodies");
  const snapshot = () => evaluate(`({ total: document.querySelector('.epic-visible-total')?.textContent || '', statuses: Array.from(document.querySelectorAll('.epic-status-counts > div'), row => [row.querySelector('dt').textContent, row.querySelector('dd').textContent]), remainder: document.querySelector('#epic-remainder').textContent, revision: document.querySelector('#epic-snapshot').textContent })`);
  const click = selector => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const navigate = async () => {
    await send("Page.navigate", { url: base + "/ui/?view=epic&epicId=" + fixture.parent });
    await until(() => evaluate("document.readyState === 'complete' && !!globalThis.PlugEpicProgress"), "embedded page"); await settled();
    assert(await evaluate("!!document.querySelector('#auth-token') && !!document.querySelector('#auth-form')"), "embedded UI must offer explicit bearer sign-in");
    assert(!(await snapshot()).total, "unauthenticated page must show no previous counts");
  };
  const signIn = async token => {
    await evaluate(`document.querySelector('#auth-token').value=${JSON.stringify(token)};document.querySelector('#auth-form').requestSubmit()`);
    await settled(); await completeWire();
    assert(await evaluate("document.querySelector('#auth-token').value === ''"), "password input must clear after submission");
    assert(await evaluate("localStorage.length === 0 && sessionStorage.length === 0"), "credentials/read state must not persist in browser storage");
  };
  const fresh = async () => { await click("#refresh"); await settled(); await completeWire(); };
  const checkProgress = async (visible, hidden, statuses) => {
    const display = await snapshot();
    assert.equal(display.total, visible + " visible children");
    assert.equal(display.remainder, hidden ? hidden + " more you cannot see" : "");
    assert.deepEqual(display.statuses, statuses);
    const reads = wire.filter(row => row.request.method === "view.run" && row.request.id.startsWith("web-ui-"));
    const row = reads.at(-1); assert(row?.authorizationPresent && !row.cookiePresent, "UI must use an actual explicit bearer without cookies");
    assert.deepEqual(row.request.params, { definition: { filter: { query: "parent=" + fixture.parent }, fields: [], groupBy: ["epic-progress"] }, _meta: { project: "ep6-epics" } });
    const result = JSON.parse(row.rawResponse).result;
    assert.deepEqual(result.items, []); assert(!Object.hasOwn(result, "nextCursor"));
    assert(result.groups.every(group => group.items.length === 0));
    assert(!row.rawResponse.includes(fixture.secret?.id || "not-a-fixture-id"), "hidden identity escaped");
    assert(!row.rawResponse.includes(fixture.hiddenProject || "not-a-project"), "hidden project escaped");
    assert(!result.groups.some(group => group.key === "status:failed"), "hidden status escaped into visible progress");
    assert(!JSON.stringify(display).includes("%"), "hidden remainder must not be a completion ratio");
    return result.asOf;
  };
  const probe = async (token, method, params) => {
    const request = { jsonrpc: "2.0", id: "chrome-probe-" + (++probeSequence), method, params };
    const response = await evaluate(`(async () => { const response = await fetch('/rpc/v1', {method:'POST',credentials:'omit',cache:'no-store',redirect:'error',headers:{'Content-Type':'application/json','Plug-RPC':'1',Authorization:'Bearer '+${JSON.stringify(token)}},body:${JSON.stringify(JSON.stringify(request))}});return {status:response.status,raw:await response.text()};})()`);
    await completeWire(); return { ...response, body: JSON.parse(response.raw) };
  };
  const get = (id, asOf) => ({ id, ...(asOf ? { asOf } : {}), _meta: { project: "ep6-visible" } });
  const progress = asOf => ({ definition: { filter: { query: "parent=" + fixture.parent }, fields: [], groupBy: ["epic-progress"] }, ...(asOf ? { asOf } : {}), _meta: { project: "ep6-epics" } });
  const screenshot = async () => { const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true }); emit({ event: "screenshot", data: png.data }); };
  await navigate();
  step("Navigated real embedded epic page; anonymous native admission refused; sign-in offered");
  await signIn(fixture.token);
  const baseline = await checkProgress(fixture.visible, fixture.hidden, fixture.full ? [["done", "1"], ["todo", "29"]] : [["todo", "1"]]);
  assert.match(await evaluate("document.querySelector('#footer-auth').textContent"), /Authenticated/);
  step("Entered disposable API key in password field; real authenticated hello/discovery/view.run; visible counts plus exact hidden remainder");
  await screenshot();
  if (fixture.full) {
    phase = "denied-and-parent-disclosure";
    const visibleRead = await probe(fixture.token, "bead.get", get(fixture.child));
    assert(visibleRead.body.result?.bead, "visible child readback failed");
    const childOnly = await probe(fixture.narrowToken, "bead.get", get(fixture.child));
    assert.deepEqual(childOnly.body.result?.bead.parent, { exists: true });
    for (const canary of [fixture.parent, fixture.parentTitle, "ep6-epics"]) assert(!childOnly.raw.includes(canary), "hidden parent disclosed more than existence");
    const deniedChild = await probe(fixture.token, "bead.get", get(fixture.secret));
    assert.equal(deniedChild.body.error?.data.code, "not_found");
    const agentRead = await probe(fixture.deniedToken, "view.run", progress());
    assert.equal(agentRead.body.error?.data.code, "not_found");
    step("Actual browser bead.get readback; hidden child not_found; hidden parent existence-only; authenticated Agent receives no fixed-policy read grant");
    phase = "hidden-status-change";
    await control("hidden-status"); await fresh();
    const changed = await checkProgress(30, 20, [["done", "1"], ["todo", "29"]]);
    assert.notEqual(changed.revision, baseline.revision, "refresh reused stale source content");
    step("Real hidden lifecycle write changed revision; refreshed visible counts and exact remainder unchanged, hidden failed status absent");
    phase = "narrow-parent-invisible";
    await signIn(fixture.narrowToken);
    assert.equal((await snapshot()).total, ""); assert.equal((await snapshot()).remainder, "");
    assert.match(await evaluate("document.querySelector('#epic-state').textContent"), /not_found/);
    assert(!(await evaluate("document.body.textContent")).includes(fixture.parentTitle));
    step("Changed to distinct child-only authenticated User; invisible epic refuses and clears every previous count");
    phase = "current-restricted-credential";
    await signIn(fixture.restrictedToken); await checkProgress(0, 50, []);
    const pinned = await probe(fixture.restrictedToken, "view.run", progress(baseline));
    assert.deepEqual(pinned.body.result?.groups.map(group => [group.key, group.count]), [["total", 0]]);
    assert.equal(pinned.body.result.groups[0].elided.count, 50);
    assert.equal(pinned.body.result.asOf.revision, baseline.revision);
    const oldReadback = await probe(fixture.restrictedToken, "bead.get", get(fixture.child, baseline));
    assert.equal(oldReadback.body.error?.data.code, "not_found");
    step("Same User with actual narrower credential scope: fresh visible=0 hidden=50; old content snapshot still uses current credential authority; old child readback not_found");
    phase = "credential-revocation";
    await signIn(fixture.token); await checkProgress(30, 20, [["done", "1"], ["todo", "29"]]);
    await control("revoke"); await fresh();
    assert.equal((await snapshot()).total, ""); assert.equal((await snapshot()).remainder, "");
    assert(await evaluate("!document.querySelector('#auth-form').hidden"), "native credential revocation must return to sign-in");
    step("Revoked actual API key at provider; same-page refresh refuses and clears prior progress");
    phase = "native-restore-nonseed-reopen";
    const restored = await control("restore");
    base = restored.base; identities.set(restored.token, fixture.principal); credentials.push(restored.token);
    await navigate(); await signIn(restored.token);
    const after = await checkProgress(30, 20, [["done", "1"], ["todo", "29"]]);
    assert.equal(after.incarnation, restored.incarnation); assert.notEqual(after.incarnation, baseline.incarnation);
    const oldSnapshot = await probe(restored.token, "view.run", progress(baseline));
    assert.equal(oldSnapshot.body.error?.data.code, "incarnation_changed");
    step("Same Chrome process navigated to native-restored, physically reopened nonseed SQLite host; reauthenticated; counts/remainder preserved; old incarnation refused");
    await screenshot();
    phase = "restored-live-hierarchy";
    await control("detach"); await fresh(); await checkProgress(29, 20, [["todo", "29"]]);
    step("Restored real Writer atomically rejected descendant cycle and detached completed child; UI fresh count=29 remainder=20 with no stale done bucket");
  }
  phase = "sign-out";
  await click("#sign-out");
  assert.equal((await snapshot()).total, ""); assert.equal((await snapshot()).remainder, "");
  assert(await evaluate("localStorage.length === 0 && sessionStorage.length === 0 && document.querySelector('#auth-token').value === ''"), "sign-out retained credentials");
  step("Signed out using the page; progress, credentials and browser storage cleared");
  await delay(500); await completeWire(); assert.deepEqual(exceptions, []);
  assert(wire.every(row => !row.cookiePresent));
  const evidence = { scope: "disposable authenticated API-key Chrome/native fixture; E3 remains 4/5", steps, wire, console: consoleEntries, exceptions };
  const encoded = JSON.stringify(evidence);
  for (const token of credentials) assert(!encoded.includes(token), "credential leaked into evidence");
  emit({ event: "evidence", data: Buffer.from(encoded).toString("base64") });
} catch (error) {
  // Preserve the actual assertion even if Chrome profile cleanup also fails.
  console.error(error.stack); process.exitCode = 1;
} finally {
  if (send && ws?.readyState === WebSocket.OPEN) await send("Browser.close").catch(() => {});
  if (ws) ws.close();
  if (chrome.exitCode === null && chrome.signalCode === null) { const stopped = once(chrome, "exit").catch(() => {}); try { process.kill(-chrome.pid, "SIGKILL"); } catch {} await stopped; }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  process.stdin.destroy();
}
