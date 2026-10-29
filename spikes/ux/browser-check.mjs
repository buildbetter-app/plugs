#!/usr/bin/env node

import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import net from "node:net";
import { once } from "node:events";

const options = parseArgs(process.argv.slice(2));
const chrome = options.chrome || "/usr/bin/google-chrome";
const url = options.url;
const domPath = options.dom;
// Keep Runtime observation open after load for a deterministic quiescence
// window.  The UX spike's browser gate promises to catch failures scheduled
// up to 250 ms after load, with margin for timer and CDP delivery jitter.
const POST_LOAD_SETTLE_MS = 500;
if (!url || !domPath) {
  throw new Error("usage: browser-check.mjs --url URL --dom PATH [--chrome PATH]");
}

const profile = await mkdtemp(`${tmpdir()}/plug-ux-browser-`);
const port = await freePort();
const child = spawn(chrome, [
  "--headless=new",
  "--disable-gpu",
  "--disable-extensions",
  "--disable-dev-shm-usage",
  "--no-first-run",
  "--no-default-browser-check",
  "--no-sandbox",
  `--remote-debugging-port=${port}`,
  "--remote-allow-origins=*",
  `--user-data-dir=${profile}`,
  "--window-size=1440,1200",
  "about:blank",
], { stdio: ["ignore", "ignore", "pipe"] });
let chromeStderr = "";
child.stderr.setEncoding("utf8");
child.stderr.on("data", (chunk) => { chromeStderr += chunk; });

let socket;
try {
  const target = await waitForTarget(port);
  socket = new WebSocket(target.webSocketDebuggerUrl);
  const cdp = await connect(socket);
  const consoleErrors = [];
  cdp.onEvent((message) => {
    if (message.method === "Runtime.consoleAPICalled" && ["error", "assert"].includes(message.params.type)) {
      consoleErrors.push(`console.${message.params.type}: ${formatArguments(message.params.args)}`);
    }
    if (message.method === "Runtime.exceptionThrown") {
      consoleErrors.push(`uncaught exception: ${formatException(message.params.exceptionDetails)}`);
    }
  });
  await cdp.command("Runtime.enable");
  await cdp.command("Page.enable");
  const load = cdp.waitForEvent("Page.loadEventFired");
  await cdp.command("Page.navigate", { url });
  await withTimeout(load, 15_000, "timed out waiting for page load");
  await sleep(POST_LOAD_SETTLE_MS);
  const evaluated = await cdp.command("Runtime.evaluate", {
    expression: "document.documentElement.outerHTML",
    returnByValue: true,
  });
  const html = evaluated.result?.value;
  if (typeof html !== "string") {
    throw new Error("browser did not return serialized DOM");
  }
  await writeFile(domPath, html);
  if (consoleErrors.length > 0) {
    throw new Error(`browser console errors for ${url}:\n${[...new Set(consoleErrors)].join("\n")}`);
  }
} finally {
  if (socket) socket.close();
  if (child.exitCode === null) {
    child.kill("SIGTERM");
    await Promise.race([once(child, "exit"), sleep(1_000)]);
  }
  if (child.exitCode === null) {
    child.kill("SIGKILL");
    await Promise.race([once(child, "exit"), sleep(1_000)]);
  }
  await rm(profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
}

function parseArgs(args) {
  const result = {};
  for (let index = 0; index < args.length; index += 1) {
    if (!args[index].startsWith("--")) throw new Error(`unexpected argument: ${args[index]}`);
    const key = args[index].slice(2);
    result[key] = args[index + 1];
    index += 1;
  }
  return result;
}

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const portNumber = address.port;
  await new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
  return portNumber;
}

async function waitForTarget(portNumber) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${portNumber}/json/list`);
      const targets = await response.json();
      const page = targets.find((target) => target.type === "page" && target.webSocketDebuggerUrl);
      if (page) return page;
    } catch {
      // Chrome is still starting.
    }
    await sleep(50);
  }
  throw new Error(`Chrome remote debugging did not start${chromeStderr ? `:\n${chromeStderr}` : ""}`);
}

function connect(ws) {
  return new Promise((resolve, reject) => {
    const pending = new Map();
    const eventHandlers = [];
    let nextId = 1;
    ws.addEventListener("open", () => resolve({
      command(method, params = {}) {
        const id = nextId++;
        return new Promise((commandResolve, commandReject) => {
          pending.set(id, { resolve: commandResolve, reject: commandReject });
          ws.send(JSON.stringify({ id, method, params }));
        });
      },
      onEvent(handler) { eventHandlers.push(handler); },
      waitForEvent(method) {
        return new Promise((eventResolve) => {
          const handler = (message) => {
            if (message.method === method) eventResolve(message);
          };
          eventHandlers.push(handler);
        });
      },
    }));
    ws.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.id && pending.has(message.id)) {
        const commandResult = pending.get(message.id);
        pending.delete(message.id);
        if (message.error) commandResult.reject(new Error(JSON.stringify(message.error)));
        else commandResult.resolve(message.result);
        return;
      }
      for (const handler of eventHandlers) handler(message);
    });
    ws.addEventListener("error", reject);
  });
}

function formatArguments(args) {
  return args.map((arg) => arg.value ?? arg.description ?? arg.type).join(" ");
}

function formatException(details) {
  return details?.text || details?.exception?.description || "unknown exception";
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function withTimeout(promise, milliseconds, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), milliseconds);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
