// A real Chrome page, not response interception or a synthetic RPC producer.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function assertHiddenParentExistenceOnly(raw, { child, parent, parentTitle }) {
  const response = JSON.parse(raw), bead = response.result?.bead;
  assert.equal(bead?.id, child.id, "authorized child's exact ID must remain unchanged");
  assert.deepEqual(bead.parent, { exists: true });
  // Match batchsql's S1 decision: only the authorized child's own ID field
  // may embed its creation parent's ID. Never waive matching text elsewhere.
  const inspected = { ...response, result: { ...response.result, bead: { ...bead } } };
  if (child.id.startsWith(parent + ".")) inspected.result.bead.id = child.id.slice(parent.length);
  const withoutChildPrefix = JSON.stringify(inspected);
  for (const canary of [parent, parentTitle, "ep6-epics"]) assert(!withoutChildPrefix.includes(canary), "hidden parent disclosed more than existence");
}

async function runBrowser() {
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
const started = Date.now(), wire = [], streams = [], consoleEntries = [], exceptions = [], steps = [];
let nativeStreams = [];
const step = text => { steps.push({ atMs: Date.now() - started, text }); emit({ event: "step", text }); };
const identities = new Map([[fixture.token, fixture.principal], [fixture.narrowToken, "user:browser-child-only"], [fixture.restrictedToken, fixture.principal], [fixture.deniedToken, "agent:owner"], [fixture.otherToken, fixture.otherPrincipal]]);
const credentials = [fixture.token, fixture.narrowToken, fixture.restrictedToken, fixture.deniedToken, fixture.otherToken].filter(Boolean);
let phase = "sign-in", base = fixture.base, probeSequence = 0;
try {
  await until(() => /DevTools listening on ws:\/\/[^\s]+/.test(stderr), "Chrome startup");
  const devtools = "http://" + new URL(stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1]).host;
  const target = await (await fetch(devtools + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl); await once(ws, "open");
  let sequence = 0; const pending = new Map(), network = new Map(), streamNetwork = new Map();
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
    } else if (event.method === "Network.requestWillBeSent" && p.request.url.endsWith("/watch/v1")) {
      const headers = Object.fromEntries(Object.entries(p.request.headers).map(([key, value]) => [key.toLowerCase(), value]));
      const row = { phase, url: p.request.url, request: p.request.postData, authorizationPresent: !!headers.authorization, cookiePresent: !!headers.cookie, principal: identities.get(String(headers.authorization || "").replace(/^Bearer /, "")) || "" };
      streams.push(row); streamNetwork.set(p.requestId, row);
    } else if (event.method === "Network.responseReceived" && streamNetwork.has(p.requestId)) {
      const row = streamNetwork.get(p.requestId); row.status = p.response.status;
      if (row.status === 401) row.principal = "";
    } else if (event.method === "Network.loadingFinished" && streamNetwork.has(p.requestId)) {
      const row = streamNetwork.get(p.requestId);
      void send("Network.getResponseBody", { requestId: p.requestId }).then(body => { row.rawResponse = body.base64Encoded ? Buffer.from(body.body, "base64").toString() : body.body; }).catch(() => { if (!row.cancelled) exceptions.push("failed to collect actual stream response body"); });
    } else if (event.method === "Network.loadingFailed" && streamNetwork.has(p.requestId)) {
      streamNetwork.get(p.requestId).cancelled = !!p.canceled;
    } else if (event.method === "Network.loadingFailed" && network.has(p.requestId)) {
      network.get(p.requestId).cancelled = !!p.canceled;
    } else if (event.method === "Network.responseReceived" && network.has(p.requestId)) {
      const row = network.get(p.requestId); row.status = p.response.status;
      if (row.status === 401) row.principal = "";
    } else if (event.method === "Network.loadingFinished" && network.has(p.requestId)) {
      const row = network.get(p.requestId);
      void send("Network.getResponseBody", { requestId: p.requestId }).then(body => {
        row.rawResponse = body.base64Encoded ? Buffer.from(body.body, "base64").toString() : body.body;
      }).catch(() => { if (!row.cancelled) exceptions.push("failed to collect actual Chrome response body"); });
    }
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable"); await send("Log.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  const evaluate = async expression => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    assert(!result.exceptionDetails, "browser evaluation failed"); return result.result.value;
  };
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "completed UI read");
  const completeWire = () => until(() => wire.every(row => typeof row.rawResponse === "string" || row.cancelled), "actual response bodies or native cancellation");
  const snapshot = () => evaluate(`({ total: document.querySelector('.epic-visible-total')?.textContent || '', statuses: Array.from(document.querySelectorAll('.epic-status-counts > div'), row => [row.querySelector('dt').textContent, row.querySelector('dd').textContent]), remainder: document.querySelector('#epic-remainder').textContent, revision: document.querySelector('#epic-snapshot').textContent })`);
  const click = selector => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const navigate = async () => {
    await send("Page.navigate", { url: base + (fixture.background ? "/ui/?view=home" : "/ui/?view=epic&epicId=" + fixture.parent) });
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
  if (fixture.background) {
    const auth = () => evaluate(`({ footer: document.querySelector('#footer-auth').textContent, signedOut: document.querySelector('#sign-out').hidden, rows: document.querySelectorAll('#changed-rows .row').length, count: document.querySelector('#changed-count').textContent, cancelHidden: document.querySelector('#cancel').hidden, refreshDisabled: document.querySelector('#refresh').disabled, stopHidden: document.querySelector('#stop-watch').hidden, frames: document.querySelectorAll('#plugin-frames iframe').length, revoked: globalThis.previousFrame?.dataset.bridgeState === 'revoked', text: document.querySelector('#presence-rows').textContent })`);
    // Inspect the real closure-owned read map through Chrome's read-only heap
    // API. No product export, response interception or source instrumentation.
    const readMap = async () => {
      const prototype = await send("Runtime.evaluate", { expression: "Map.prototype" });
      const maps = await send("Runtime.queryObjects", { prototypeObjectId: prototype.result.objectId });
      const selected = await send("Runtime.callFunctionOn", { objectId: maps.objects.objectId, functionDeclaration: "function(){return Array.from(this).find(map=>Array.from(map.keys()).some(key=>typeof key==='string'&&key.startsWith('plug:seen:')))}" });
      assert(selected.result.objectId, "actual authenticated read-marker map must exist before failure");
      return selected.result.objectId;
    };
    const mapSize = async objectId => (await send("Runtime.callFunctionOn", { objectId, functionDeclaration: "function(){return this.size}", returnByValue: true })).result.value;
    const bridge = async (params, wait = true) => {
      const { frameTree } = await send("Page.getFrameTree");
      const nodes = []; const walk = node => { nodes.push(node); (node.childFrames || []).forEach(walk); }; walk(frameTree);
      const frame = nodes.find(node => node.frame.url === base + "/ui/frame-hosts/auth-probe/index.html");
      assert(frame, "actual trusted frame wrapper must exist");
      const { executionContextId } = await send("Page.createIsolatedWorld", { frameId: frame.frame.id, worldName: "background-auth-browser-probe" });
      const id = "background-frame-" + (++probeSequence);
      const request = { jsonrpc: "2.0", id, method: "bead.get", params };
      const expression = `globalThis.responses ||= {}; addEventListener('message', event => {if(event.source===parent&&event.data?.id===${JSON.stringify(id)})globalThis.responses[${JSON.stringify(id)}]=event.data;});parent.postMessage(${JSON.stringify(request)},location.origin);`;
      await send("Runtime.evaluate", { expression, contextId: executionContextId });
      if (!wait) return;
      let response;
      await until(async () => { const value = await send("Runtime.evaluate", { expression: `globalThis.responses[${JSON.stringify(id)}]`, contextId: executionContextId, returnByValue: true }); response = value.result.value; return !!response; }, "actual frame bridge response");
      return response;
    };
    const homeConnected = async () => {
      await until(() => evaluate("document.querySelector('#watch-state').textContent.includes('connected') && document.querySelectorAll('#changed-rows .row').length === 1"), "actual native saved view and watch");
      assert.equal((await auth()).count, "1");
    };
    const homeReady = async () => { await settled(); await completeWire(); await homeConnected(); };
    await navigate(); await signIn(fixture.token); await homeReady();
    if (["bridge-expired", "stop-watch-expired"].includes(fixture.background)) {
      const expiring = await control("expiring-key"); credentials.push(expiring.token); identities.set(expiring.token, fixture.principal);
      await signIn(expiring.token); await homeReady();
    }
    const markers = await readMap(); assert((await mapSize(markers)) > 0);
    await evaluate("globalThis.previousFrame = document.querySelector('#plugin-frames iframe')");
    step("Authenticated real saved Home view and native watch; closure read markers and active frame bridge observed before background failure");
    phase = fixture.background;
    if (fixture.background.startsWith("opening-")) {
      await click('.tab[data-view="presence"]'); await settled(); await completeWire();
      await until(async () => (await control("stats")).presence.length === 0, "previous native session closed before held open");
      await control("arm-open-reply"); await click('.tab[data-view="home"]'); await control("wait-held");
      const allocated = await control("stats");
      assert(allocated.heldSession && allocated.heldSessionActive, "held reply must follow a real native session allocation");
      assert.equal(allocated.presence.length, 1); assert.equal(allocated.presence[0].sessionId, allocated.heldSession);
      assert.equal(allocated.presence[0].principal.id, fixture.principal);
      assert(!wire.some(row => row.request.method === "session.open" && row.phase === phase && typeof row.rawResponse === "string"), "native allocation reply must still be held from Chrome");
      if (fixture.background === "opening-sign-out") {
        await click("#sign-out");
        const cleared = await auth();
        assert(cleared.signedOut && cleared.cancelHidden && !cleared.refreshDisabled && cleared.stopHidden);
        assert.equal(cleared.footer, "Sign-in required"); assert.equal(cleared.rows, 0); assert.equal(cleared.count, "—");
        assert.equal(cleared.frames, 0); assert(cleared.revoked); assert.equal(await mapSize(markers), 0);
      } else {
        const switched = await send("Runtime.callFunctionOn", { objectId: markers, functionDeclaration: "function(token){document.querySelector('#auth-token').value=token;document.querySelector('#auth-form').requestSubmit();return this.size}", arguments: [{ value: fixture.otherToken }], returnByValue: true });
        assert.equal(switched.result.value, 0, "switch clears prior read markers even while native open reply is held");
        await settled(); await homeConnected();
        assert((await auth()).revoked, "switch revokes the prior frame before the delayed open reply");
      }
      const beforeReply = await control("stats");
      assert(beforeReply.heldSessionActive && beforeReply.presence.some(row => row.sessionId === allocated.heldSession), "old native allocation must survive until its reply is delivered");
      phase = "late-opening-cleanup"; await control("release");
      await until(async () => { const stats = await control("stats"); return !stats.heldSessionActive && !stats.presence.some(row => row.sessionId === allocated.heldSession); }, "late opening allocation and presence released by originating identity");
      await completeWire();
      const close = wire.find(row => row.phase === phase && row.request.method === "session.close" && row.request.params.sessionId === allocated.heldSession);
      assert(close?.status === 200 && !JSON.parse(close.rawResponse).error, "late native session.close must succeed, not await idle expiry");
      assert.equal(close.principal, fixture.principal);
      const oldCalls = wire.filter(row => row.phase === phase && row.principal === fixture.principal);
      assert.deepEqual(oldCalls.map(row => row.request.method), ["session.close"], "old bearer after identity teardown is confined to same-origin allocation cleanup");
      assert(oldCalls.every(row => row.url === base + "/rpc/v1" && row.authorizationPresent && !row.cookiePresent));
      const remaining = (await control("stats")).presence;
      if (fixture.background === "opening-sign-out") {
        assert.deepEqual(remaining, []); assert.equal((await auth()).footer, "Sign-in required"); assert.equal((await auth()).rows, 0);
      } else {
        assert.equal(remaining.length, 1); assert.equal(remaining[0].principal.id, fixture.otherPrincipal);
        assert.match((await auth()).footer, /Authenticated/); assert.equal((await auth()).rows, 1);
        assert((await bridge({ id: { id: fixture.parent, branch: 0 } })).result?.bead);
        assert.equal(wire.filter(row => row.request.method === "bead.get").at(-1).principal, fixture.otherPrincipal);
      }
      step("Held actual native session.open reply released after " + fixture.background + "; originating identity closes allocation immediately, old session/presence absent, replacement identity unchanged");
    } else if (fixture.background === "stale-cleanup-401") {
      await control("arm-close"); await click("#stop-watch"); await control("wait-held");
      await until(() => wire.some(row => row.phase === phase && row.request.method === "event.unsubscribe" && row.status === 200 && typeof row.rawResponse === "string"), "actual unsubscribe completed before old close authentication failure");
      await control("revoke");
      const switched = await send("Runtime.callFunctionOn", { objectId: markers, functionDeclaration: "function(token){document.querySelector('#auth-token').value=token;document.querySelector('#auth-form').requestSubmit();return this.size}", arguments: [{ value: fixture.otherToken }], returnByValue: true });
      assert.equal(switched.result.value, 0); await settled(); await homeConnected();
      await control("release"); await completeWire(); await delay(100);
      const refused = wire.find(row => row.phase === phase && row.request.method === "session.close");
      assert.equal(refused?.status, 401, "prior-identity cleanup must encounter actual native key revocation");
      assert.match((await auth()).footer, /Authenticated/); assert.equal((await auth()).rows, 1); assert((await auth()).revoked);
      assert((await mapSize(markers)) > 0, "stale cleanup 401 must not clear the replacement read markers");
      assert((await bridge({ id: { id: fixture.parent, branch: 0 } })).result?.bead);
      assert.equal(wire.filter(row => row.request.method === "bead.get").at(-1).principal, fixture.otherPrincipal);
      step("Actual revoked old-key session.close 401 arrives after distinct-principal switch; current identity, rows, read markers and new bridge remain active");
    } else {
    if (fixture.background.startsWith("stop-watch-")) {
      await control(fixture.background === "stop-watch-expired" ? "wait-expired" : "revoke");
      await click("#stop-watch");
    } else if (fixture.background === "watch-stream-401" || fixture.background === "non-auth-and-switch") {
      await click('.tab[data-view="presence"]'); await settled(); await completeWire();
      await control("arm-stream"); await click('.tab[data-view="home"]'); await control("wait-held");
      if (fixture.background === "watch-stream-401") await control("revoke");
      else await control("close-session");
      await control("release");
    } else if (fixture.background === "watch-read-401") {
      await control("arm-get"); await bridge({ id: { id: fixture.parent, branch: 0 } }, false); await control("wait-held");
      await control("revoke"); await control("write-parent");
    } else {
      await click('.tab[data-view="presence"]'); await settled(); await completeWire();
      await control("arm-view"); await click('.tab[data-view="home"]'); await control("wait-held");
      if (fixture.background === "bridge-expired") await control("wait-expired");
      else await control("revoke");
      await bridge({ id: { id: fixture.parent, branch: 0 } }, false);
    }
    if (fixture.background === "non-auth-and-switch") {
      await settled(); await completeWire();
      assert.match((await auth()).footer, /Authenticated/);
      assert((await mapSize(markers)) > 0, "non-auth stream 404 must retain identity/read markers");
      assert.equal((await auth()).revoked, false);
      const refused = await bridge({ id: fixture.secret }); assert.equal(refused.error?.data.code, "not_found");
      const readable = await bridge({ id: { id: fixture.parent, branch: 0 } }); assert(readable.result?.bead);
      assert.match((await auth()).footer, /Authenticated/);
      step("Actual closed-session watch 404 and native bridge not_found preserve current identity, read markers and bridge; subsequent granted read succeeds");
      await control("arm-get"); await bridge({ id: { id: fixture.parent, branch: 0 } }, false); await control("wait-held");
      phase = "prior-identity-switch";
      const switched = await send("Runtime.callFunctionOn", { objectId: markers, functionDeclaration: "function(token){document.querySelector('#auth-token').value=token;document.querySelector('#auth-form').requestSubmit();return this.size}", arguments: [{ value: fixture.otherToken }], returnByValue: true });
      assert.equal(switched.result.value, 0, "identity switch clears old marker map before new reads");
      await homeReady();
      assert.equal((await auth()).revoked, true, "old bridge must be revoked by the switch");
      await control("release"); await delay(100); await completeWire();
      const current = await bridge({ id: { id: fixture.parent, branch: 0 } }); assert(current.result?.bead);
      assert.equal(wire.filter(row => row.request.method === "bead.get" && !row.cancelled).at(-1).principal, fixture.otherPrincipal);
      assert.match((await auth()).footer, /Authenticated/); assert.equal((await auth()).rows, 1);
      assert((await control("stats")).cancelled, "switch must cancel the actual old native request");
      step("Distinct replacement identity remains connected after actual prior-identity request cancellation; old bridge/read map revoked, new authenticated bridge succeeds");
    } else {
      // This assertion precedes release of held normal work, refresh, sign-out
      // or reauthentication, so none can serve as the teardown fallback.
      await until(async () => (await auth()).footer === "Sign-in required", "immediate background authentication identity teardown");
      const cleared = await auth();
      assert(cleared.signedOut && cleared.cancelHidden && !cleared.refreshDisabled && cleared.stopHidden);
      assert.equal(cleared.rows, 0); assert.equal(cleared.count, "—"); assert.equal(cleared.frames, 0); assert(cleared.revoked); assert.equal(cleared.text, "");
      assert.equal(await mapSize(markers), 0, "background auth failure must clear actual closure read markers");
      assert(await evaluate("localStorage.length === 0 && sessionStorage.length === 0 && document.querySelector('#auth-token').value === ''"));
      if (fixture.background !== "watch-stream-401" && !fixture.background.startsWith("stop-watch-")) {
        await control("release");
        assert((await control("stats")).cancelled, "background auth teardown must cancel active actual native work");
      }
      await completeWire();
      const refused = fixture.background === "watch-stream-401" ? streams.find(row => row.phase === phase && row.status === 401) : wire.find(row => row.phase === phase && row.status === 401 && row.request.method === (fixture.background.startsWith("stop-watch-") ? "session.close" : fixture.background === "watch-read-401" ? "view.run" : "bead.get"));
      assert(refused, "background failure must be an actual native HTTP 401");
      if (["bridge-expired", "stop-watch-expired"].includes(fixture.background)) assert.equal(JSON.parse(refused.rawResponse).error.data.code, "token_expired");
      if (fixture.background.startsWith("stop-watch-")) {
        const cleanupOnly = wire.filter(row => row.phase === phase);
        assert.deepEqual(cleanupOnly.map(row => row.request.method).sort(), ["event.unsubscribe", "session.close"]);
        assert(cleanupOnly.every(row => row.status === 401), "Stop alone must expose native cleanup authentication failures");
        step("Stop live changes alone after actual key revocation/expiry tears down identity; no refresh, normal RPC or bridge failure is a fallback");
      }
      step("Background " + fixture.background + " native 401 alone cleared credential presentation, actual closure read map, all rows and bridges; active work/watch aborted before any normal-read fallback");
      await screenshot();
      // Only after the teardown proof: a new explicit read must carry no old
      // bearer. Its 401 cannot be the event that caused the assertions above.
      phase = "post-teardown-no-bearer"; await fresh();
      assert(wire.filter(row => row.phase === phase).every(row => !row.authorizationPresent));
      step("Subsequent explicit read carries no previous bearer after background teardown");
    }
    }
    await click("#sign-out");
    await until(async () => (await control("stats")).active === 0, "all actual native streams stopped");
    await completeWire();
    await until(() => streams.every(row => typeof row.rawResponse === "string" || row.cancelled), "actual stream completions");
    nativeStreams = (await control("streams")).streams;
    assert.equal(streams.length, nativeStreams.length, "every native watch attachment must be a Chrome request");
    for (const row of streams) {
      const native = nativeStreams.find(entry => entry.request === row.request);
      assert(native); assert.equal(row.status, native.status); assert.equal(row.principal, native.principal);
      if (typeof row.rawResponse === "string") assert.equal(row.rawResponse, native.body, "actual native stream response-byte parity");
      else {
        // The app also aborts unread error bodies when a watch is refused.
        // Match the actual status/principal, and claim no unavailable bytes.
        assert(row.cancelled && [200, 401, 404].includes(row.status), "missing Chrome body requires actual cancellation with the correlated native status");
        if (row.status === 401) assert.equal(native.body, "unauthenticated\n");
        if (row.status === 404) assert.equal(native.body, "session not found\n");
      }
      assert(row.authorizationPresent && !row.cookiePresent);
    }
    assert(streams.some(row => row.status === (fixture.background === "watch-stream-401" ? 401 : fixture.background === "non-auth-and-switch" ? 404 : 200)));
    step("Actual Chrome/native watch attachment request/status/principal correlation complete; complete response bytes match, cancelled bodies explicitly recorded without byte-parity claims");
  } else {
  await navigate();
  step("Navigated real embedded epic page; anonymous native admission refused; sign-in offered");
  await signIn(fixture.token);
  const baseline = await checkProgress(fixture.visible, fixture.hidden, fixture.lifecycle ? [["done", "1"], ["todo", "29"]] : [["todo", "1"]]);
  assert.match(await evaluate("document.querySelector('#footer-auth').textContent"), /Authenticated/);
  step("Entered disposable API key in password field; real authenticated hello/discovery/view.run; visible counts plus exact hidden remainder");
  await screenshot();
  if (fixture.lifecycle) {
    phase = "denied-and-parent-disclosure";
    const visibleRead = await probe(fixture.token, "bead.get", get(fixture.child));
    assert(visibleRead.body.result?.bead, "visible child readback failed");
    const childOnly = await probe(fixture.narrowToken, "bead.get", get(fixture.child));
    assertHiddenParentExistenceOnly(childOnly.raw, fixture);
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
  if (!fixture.lifecycle) {
    await click('.tab[data-view="home"]'); await settled(); await completeWire();
    assert(await evaluate("localStorage.length === 0 && sessionStorage.length === 0"), "authenticated displayed-row markers must stay in tab memory");
    step("Opened actual authenticated home rows; displayed-row markers remain in tab memory");
  }
  phase = "sign-out";
  await click("#sign-out");
  assert.equal((await snapshot()).total, ""); assert.equal((await snapshot()).remainder, "");
  assert(await evaluate("localStorage.length === 0 && sessionStorage.length === 0 && document.querySelector('#auth-token').value === ''"), "sign-out retained credentials");
  step("Signed out using the page; progress, credentials and browser storage cleared");
  }
  await delay(500); await completeWire(); assert.deepEqual(exceptions, []);
  assert(wire.every(row => !row.cookiePresent));
  const evidence = { population: fixture.population || 3, background: fixture.background, scope: "disposable authenticated API-key Chrome/native fixture; E3 remains 4/5", steps, wire, streams, nativeStreams, console: consoleEntries, exceptions };
  const encoded = JSON.stringify(evidence);
  for (const token of credentials) assert(!encoded.includes(token), "credential leaked into evidence");
  emit({ event: "evidence", data: Buffer.from(encoded).toString("base64") });
} catch (error) {
  // Preserve the actual assertion even if Chrome profile cleanup also fails.
  console.error(error.stack); process.exitCode = 1;
  await delay(100);
  const partial = JSON.stringify({ population: fixture.population || 3, background: fixture.background, scope: "failed disposable authenticated browser fixture; no qualification", steps, wire, streams, nativeStreams, console: consoleEntries, exceptions });
  if (credentials.every(token => !partial.includes(token))) emit({ event: "evidence", data: Buffer.from(partial).toString("base64") });
} finally {
  if (send && ws?.readyState === WebSocket.OPEN) await Promise.race([send("Browser.close").catch(() => {}), delay(2000)]);
  if (ws) ws.close();
  if (chrome.exitCode === null && chrome.signalCode === null) { const stopped = once(chrome, "exit").catch(() => {}); try { process.kill(-chrome.pid, "SIGKILL"); } catch {} await stopped; }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  process.stdin.destroy();
}
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) await runBrowser();
