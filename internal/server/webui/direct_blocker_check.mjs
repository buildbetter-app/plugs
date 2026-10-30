import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const [base, chromePath, casesPath] = process.argv.slice(2);
const fixtures = JSON.parse(await readFile(casesPath, "utf8"));
const profile = await mkdtemp(path.join(os.tmpdir(), "plug-blocker-consumer-"));
const chrome = spawn(chromePath, ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--disable-sync", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
let stderr = "", ws;
chrome.stderr.on("data", chunk => { stderr += chunk.toString(); });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, description) {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) { if (await fn()) return; await delay(25); }
  throw new Error("Timed out: " + description);
}
try {
  await until(() => /DevTools listening on ws:\/\/[^\s]+/.test(stderr), "browser startup");
  const debug = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1];
  const target = await (await fetch("http://" + new URL(debug).host + "/json/new?about:blank", { method: "PUT" })).json();
  ws = new WebSocket(target.webSocketDebuggerUrl); await once(ws, "open");
  let sequence = 0;
  const pending = new Map(), requests = [], errors = [], evidence = [];
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
  });
  ws.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data.toString());
    if (event.id) {
      const promise = pending.get(event.id); pending.delete(event.id);
      if (promise) event.error ? promise.reject(new Error("browser command refused")) : promise.resolve(event.result);
    } else if (event.method === "Network.requestWillBeSent" && event.params.request.url.includes("/rpc")) requests.push(JSON.parse(event.params.request.postData));
    else if (event.method === "Runtime.exceptionThrown") errors.push(event.params.exceptionDetails);
  });
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: false });
  const evaluate = async expression => {
    const result = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    assert(!result.exceptionDetails, "browser assertion expression failed"); return result.result.value;
  };
  const settled = () => until(() => evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"), "explanation read completion");
  const rendered = () => evaluate(`({
    state: document.querySelector('#connection-state').textContent,
    text: document.querySelector('#explain-state').textContent,
    rows: Array.from(document.querySelectorAll('.explain-direct-blockers li'), row => ({
      state: row.dataset.state, ref: row.querySelector('.blocker-ref').textContent,
      href: row.querySelector('a')?.href || '', reason: row.querySelector('.blocker-reason').textContent,
      verdict: row.querySelector('.blocker-verdict').textContent,
    })),
    hidden: document.querySelector('.explain-hidden-blockers')?.textContent || '',
    unavailable: !!document.querySelector('.direct-blockers-unavailable'),
    injected: !!document.querySelector('#explain-state img, #explain-state script') || globalThis.blockerExecuted === true,
    overflow: document.documentElement.scrollWidth > innerWidth,
  })`);
  for (const fixture of fixtures) {
    const url = base + "/fixtures/" + fixture.name + "/ui/?view=explain&itemId=pl-dependent";
    await send("Page.navigate", { url });
    await until(() => evaluate(`location.href === ${JSON.stringify(url)} && document.readyState === 'complete' && !!document.querySelector('#refresh')`), "fixture document");
    await settled();
    const result = await rendered(); evidence.push({ name: fixture.name, result });
    assert.equal(result.injected, false); assert.equal(result.overflow, false);
    if (fixture.refused) {
      assert.equal(result.state, "Unavailable", fixture.name); assert.match(result.text, /invalid_response/);
      assert.equal(result.rows.length, 0); assert(!result.text.includes("pl-secret") && !result.text.includes("private-reason"));
    } else {
      assert.equal(result.state, "Connected", fixture.name);
      assert.equal(result.unavailable, !!fixture.absent);
      const edges = fixture.direct?.edges || [];
      assert.equal(result.rows.length, edges.length);
      for (let i = 0; i < edges.length; i++) {
        const edge = edges[i], row = result.rows[i];
        assert.equal(row.state, edge.state); assert.equal(row.reason, edge.reason || "No direct reason supplied.");
        assert(row.verdict.startsWith(({ satisfied: "Satisfied", unsatisfied: "Unsatisfied", unknown: "Unknown" })[edge.state]));
        if (Object.hasOwn(edge.blocker, "key")) { assert.equal(row.ref, "Key " + edge.blocker.key); assert.equal(row.href, ""); }
        else if (fixture.name === "branch") { assert.equal(row.ref, "pl-branch · branch 9007199254740993"); assert.equal(row.href, ""); }
        else if (fixture.unselectable) { assert.equal(row.ref, edge.blocker.id); assert.equal(row.href, ""); }
        else { assert.equal(row.ref, edge.blocker.id); const link = new URL(row.href); assert.equal(link.origin, new URL(base).origin); assert.equal(link.searchParams.get("itemId"), edge.blocker.id); assert.equal(link.searchParams.get("view"), "explain"); }
      }
      const hidden = fixture.direct?.hiddenCount || fixture.hidden || 0;
      assert.equal(result.hidden, hidden ? hidden + " blockers cannot be read with current permissions." : "");
      if (fixture.name === "empty") assert.match(result.text, /No direct blocker edges in this snapshot/);
      if (fixture.name === "hidden") { assert.equal(result.rows.length, 0); assert.match(result.text, /Only unreadable/); }
      if (fixture.select) {
        await evaluate("document.querySelector('.blocker-ref').click()"); await settled();
        assert.equal(requests.at(-1).params.itemId, fixture.select, "link must select the actual canonical blocker ID");
      }
      if (fixture.unselectable) {
        const before = requests.length;
        await evaluate("document.querySelector('.blocker-ref').click()");
        await delay(75);
        assert.equal(requests.length, before, "unsupported references must not request a transformed target");
        assert.equal(await evaluate("document.querySelector('#explain-id').value"), "pl-dependent");
      }
      if (fixture.refresh) {
        await evaluate("document.querySelector('#refresh').click()"); await settled();
        const revoked = await rendered(); assert.equal(revoked.state, "Unavailable"); assert.equal(revoked.rows.length, 0);
        assert(!revoked.text.includes("pl-before-revocation")); evidence.push({ name: "revoked-after-refresh", result: revoked });
      }
    }
    console.log("PASS direct_blocker_consumer_fixture=" + fixture.name);
  }
  assert.equal(errors.length, 0);
  for (const request of requests) {
    assert(["server.hello", "rpc.methods", "view.explain"].includes(request.method), "fixtures must remain read only");
    if (request.method === "view.explain") { assert.equal(request.params.viewId, "view-blocked"); assert.deepEqual(request.params._meta, { project: "web" }); }
  }
  if (process.env.TEST_UNDECLARED_OUTPUTS_DIR) {
    await mkdir(process.env.TEST_UNDECLARED_OUTPUTS_DIR, { recursive: true });
    await writeFile(path.join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, "direct-blocker-consumer.json"), JSON.stringify({ requests, evidence, errors }, null, 2));
  }
  console.log("PASS direct_blocker_consumer_fixtures=" + fixtures.length + " skipped=0 qualification=fixture_only");
} finally {
  if (ws) ws.close();
  if (chrome.exitCode === null) { const exited = once(chrome, "exit"); chrome.kill("SIGTERM"); await exited; }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
