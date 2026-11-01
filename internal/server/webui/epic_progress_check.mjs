// Drives the real embedded page. Presentation mode uses explicit normalized
// display inputs and cannot qualify server authorization or view semantics.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, chromePath, mode, evidence] = process.argv.slice(2);
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-epic-progress-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--disable-sync", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws;
chrome.stderr.on("data", chunk => { stderr += chunk.toString(); });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, description) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) { if (await fn()) return; await delay(25); }
  throw new Error("Timed out: " + description);
}
try {
  await until(() => /DevTools listening on ws:\/\/[^\s]+/.test(stderr), "browser startup");
  const address = "http://" + new URL(stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1]).host;
  const target = await (await fetch(address + "/json/new?about:blank", { method: "PUT" })).json();
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
      const job = pending.get(event.id); pending.delete(event.id);
      if (job) event.error ? job.reject(new Error("browser command refused")) : job.resolve(event.result);
    } else if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
    else if (event.method === "Network.requestWillBeSent" && event.params.request.url.endsWith("/rpc/v1")) requests.push(JSON.parse(event.params.request.postData));
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  const evaluate = async expression => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    assert(!result.exceptionDetails, "browser assertion expression failed: " + JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "read completion");
  const click = selector => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  const value = selector => evaluate(`document.querySelector(${JSON.stringify(selector)})?.textContent || ''`);
  const snapshot = () => evaluate(`({
    total: document.querySelector('.epic-visible-total')?.textContent || '',
    statuses: Array.from(document.querySelectorAll('.epic-status-counts > div'), row => [row.querySelector('dt').textContent, row.querySelector('dd').textContent]),
    remainder: document.querySelector('#epic-remainder').textContent,
    revision: document.querySelector('#epic-snapshot').textContent,
    overflow: document.documentElement.scrollWidth > innerWidth,
    injected: !!document.querySelector('#epic-counts img, #epic-counts script') || globalThis.epicInjected === true,
  })`);
  const screenshot = async name => {
    const png = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
    await writeFile(path.join(evidence, name + ".png"), Buffer.from(png.data, "base64"));
  };
  await send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await send("Page.navigate", { url: base + "/ui/?view=epic" });
  await until(() => evaluate("document.readyState === 'complete' && !!globalThis.PlugEpicProgress && !!document.querySelector('#refresh')"), "epic page");
  await settled();

  if (mode === "presentation") {
    assert.match(await value("#epic-state"), /Enter an epic ID/);
    const summary = { visibleCount: 3, hiddenCount: 7, counts: [{ label: "Doing", count: 1 }, { label: "Resolved", count: 2 }], asOf: { revision: "9007199254740993", incarnation: "fixture-incarnation" } };
    const show = input => evaluate(`(() => { try { PlugEpicProgress.show(${JSON.stringify(input)}, 5); return ''; } catch (error) { return error.code; } })()`);
    assert.equal(await show(summary), "");
    assert.deepEqual((await snapshot()).statuses, [["Doing", "1"], ["Resolved", "2"]]);
    assert.equal((await snapshot()).total, "3 visible children");
    assert.equal((await snapshot()).remainder, "7 more you cannot see");
    assert.equal((await snapshot()).revision, "Snapshot rev 9007199254740993");
    assert(!(await value("#epic-counts")).includes("10"), "hidden children must not enter a visible denominator");
    await screenshot("epic-progress-desktop");
    console.log("PASS presentation visible-only counts and exact hidden remainder; no completion ratio");

    const hostile = { ...summary, counts: [{ label: '<img src=x onerror="globalThis.epicInjected=true">' + "x".repeat(120), count: 3 }] };
    await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: false });
    assert.equal(await show(hostile), "");
    assert.equal((await snapshot()).injected, false);
    assert.equal((await snapshot()).overflow, false);
    await screenshot("epic-progress-mobile-text");
    console.log("PASS presentation status labels are text; mobile has no horizontal overflow");

    const invalid = [
      { ...summary, hiddenCount: -1 }, { ...summary, hiddenCount: 0.5 },
      { ...summary, visibleCount: 4 }, { ...summary, counts: Array(6).fill({ label: "Doing", count: 1 }), visibleCount: 6 },
      { ...summary, counts: [{ label: "Doing", count: Number.MAX_SAFE_INTEGER + 1 }], visibleCount: Number.MAX_SAFE_INTEGER + 1 },
      { ...summary, asOf: { revision: "01", incarnation: "fixture" } },
      { ...summary, asOf: { revision: Number.MAX_SAFE_INTEGER + 1, incarnation: "fixture" } },
    ];
    for (const item of invalid) {
      assert.equal(await show(item), "invalid_response");
      assert.equal((await snapshot()).total, "");
      assert.equal((await snapshot()).remainder, "");
      assert.equal((await snapshot()).revision, "");
    }
    assert.equal(await show({ ...summary, visibleCount: 0, hiddenCount: 0, counts: [] }), "");
    assert.match(await value("#epic-counts"), /No visible direct children/);
    assert.equal((await snapshot()).remainder, "");
    await show({ ...summary, visibleCount: 0, counts: [] });
    assert.equal((await snapshot()).total, "0 visible children");
    assert.equal((await snapshot()).statuses.length, 0);
    assert.equal((await snapshot()).remainder, "7 more you cannot see");
    await show(summary); await click("#refresh"); await settled();
    assert.equal((await snapshot()).total, "");
    assert.equal((await snapshot()).remainder, "");
    await evaluate("document.querySelector('#epic-id').value = 'pl-fixture'; document.querySelector('#epic-form').requestSubmit()");
    await settled();
    assert.match(await value("#epic-state"), /method_not_found/);
    assert.equal((await snapshot()).total, "");
    assert(requests.every(request => ["server.hello", "rpc.methods"].includes(request.method)), "presentation must not issue fallback counts or graph reads");
    console.log("PASS presentation malformed/capped inputs refuse; refresh and missing capability clear old counts");
  } else if (mode === "contract") {
    const cases = await (await fetch(base + "/fixtures/cases.json")).json();
    for (const fixture of cases) {
      const url = base + "/fixtures/" + fixture.name + "/ui/?view=epic&epicId=pl-parent";
      const before = requests.filter(request => request.method === "view.run").length;
      await send("Page.navigate", { url });
      await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "contract page");
      await settled();
      const result = await snapshot();
      const state = await value("#connection-state");
      assert.equal(result.injected, false, fixture.name);
      assert.equal(result.overflow, false, fixture.name);
      assert(!(/unreadable-child|unreadable-status|unreadable-project/.test(await value("main"))), "no identities/statuses from malformed data or error hints");
      if (fixture.code) {
        assert.equal(state, "Unavailable", fixture.name);
        assert((await value("#epic-state")).includes(fixture.code), fixture.name + " preserves its typed refusal");
        assert.equal(result.total, "", fixture.name);
        assert.equal(result.remainder, "", fixture.name);
      } else {
        assert.equal(state, "Connected", fixture.name);
        assert.equal(result.total, fixture.total + " visible children", fixture.name);
        assert.deepEqual(result.statuses, fixture.statuses, fixture.name);
        assert.equal(result.remainder, fixture.hidden ? fixture.hidden + " more you cannot see" : "", fixture.name);
      }
      assert.equal(requests.filter(request => request.method === "view.run").length, before + 1, "a hidden remainder must not start a follow-up read");
      if (fixture.refresh) {
        await click("#refresh");
        if (fixture.refresh === "slow") {
          await until(() => requests.filter(request => request.method === "view.run").length === before + 2, "in-flight read to cancel");
          assert.equal((await snapshot()).total, "", "old counts clear before a pending permission read");
          await click("#cancel"); await settled();
          assert.equal(await value("#connection-state"), "Cancelled");
          assert.equal((await snapshot()).total, "");
          assert.equal((await snapshot()).remainder, "");
        } else {
          await settled();
          if (fixture.refresh === "empty") {
            assert.equal((await snapshot()).total, "0 visible children");
            assert.deepEqual((await snapshot()).statuses, []);
          } else {
            assert.equal(await value("#connection-state"), "Unavailable");
            assert((await value("#epic-state")).includes(fixture.refresh));
            assert.equal((await snapshot()).total, "");
          }
          assert.equal((await snapshot()).remainder, "", "refresh must remove the prior hidden remainder");
        }
        assert(!(/unreadable-child|unreadable-status/.test(await value("main"))));
      }
      console.log("PASS wire consumer " + fixture.name);
    }
    const before = requests.filter(request => request.method === "view.run").length;
    await evaluate("document.querySelector('#epic-id').value = 'pl-parent AND status=unreadable-status'; document.querySelector('#epic-form').requestSubmit()");
    await settled();
    assert.equal(requests.filter(request => request.method === "view.run").length, before, "an ID must not inject filter syntax");
    assert.equal((await snapshot()).total, "");
    assert(requests.every(request => ["server.hello", "rpc.methods", "view.run"].includes(request.method)), "no fallback graph or write RPCs");
    console.log("PASS wire consumer canonical parent requests, no hidden pagination, no graph/policy fallback");
  } else {
    throw new Error("unknown epic browser mode");
  }
  assert.deepEqual(errors, [], "no uncaught browser errors");
} finally {
  if (ws) ws.close();
  const stopped = once(chrome, "exit");
  chrome.kill("SIGTERM");
  await Promise.race([stopped, delay(2000)]);
  if (chrome.exitCode === null) { chrome.kill("SIGKILL"); await stopped; }
  await rm(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}
