(function () {
  "use strict";

  const state = { config: null, hello: null, methods: new Set(), activeView: "home", work: null, sequence: 0, home: null, next: null, blocked: null, bridges: [] };
  const $ = (id) => document.getElementById(id);
  const text = (node, value) => { node.textContent = value == null ? "" : String(value); };
  const clear = (node) => node.replaceChildren();
  const integer = (value) => /^(0|[1-9][0-9]*)$/.test(String(value));
  const count = (value) => Number.isSafeInteger(value) && value >= 0 ? value : 0;

  class RPCError extends Error {
    constructor(message, code) { super(message); this.name = "RPCError"; this.code = code || "unavailable"; }
  }
  function failure(error) { return error && error.code ? error.code + ": " + error.message : (error && error.message || "The server could not complete this read."); }
  function setConnection(kind, label) { $("connection-state").className = "status-pill status-" + kind; text($("connection-state"), label); }
  function setMessage(message, kind) { $("global-message").className = "global-message" + (kind ? " is-" + kind : ""); text($("global-message"), message); }
  function message(node, kind, title, detail) {
    const box = document.createElement("div"); box.className = "state state-" + kind;
    const strong = document.createElement("strong"); text(strong, title); box.appendChild(strong);
    const copy = document.createElement("span"); text(copy, detail); box.appendChild(copy);
    node.replaceChildren(box);
  }

  // Preserve exact revision bytes. New browsers expose JSON.parse's original
  // primitive source; older ones explicitly refuse an unsafe integer.
  function parseJSON(raw) {
    return JSON.parse(raw, (key, value, context) => {
      if ((key === "revision" || key === "ontologyRevision") && typeof value === "number") {
        if (context && integer(context.source)) return context.source;
        if (!Number.isSafeInteger(value) || value < 0) throw new RPCError("This browser cannot preserve the server revision.", "revision_unavailable");
        return String(value);
      }
      return value;
    });
  }
  function asOfParams(at) {
    if (!at || !integer(at.revision) || typeof at.incarnation !== "string" || !at.incarnation) throw new RPCError("The server returned no pinned snapshot.", "invalid_response");
    let revision;
    if (typeof JSON.rawJSON === "function") revision = JSON.rawJSON(String(at.revision));
    else {
      revision = Number(at.revision);
      if (!Number.isSafeInteger(revision)) throw new RPCError("This browser cannot send the exact revision.", "revision_unavailable");
    }
    return { revision, incarnation: at.incarnation };
  }
  async function rpc(method, params, signal, requestedProject) {
    const input = Object.assign({}, params || {});
    const project = typeof requestedProject === "string" && requestedProject ? requestedProject : state.config.project;
    if (method !== "server.hello") input._meta = { project };
    const id = "web-ui-" + (++state.sequence);
    const response = await fetch(state.config.rpcPath, {
      method: "POST", mode: "same-origin", credentials: "omit", redirect: "error", cache: "no-store", signal,
      headers: { "Accept": "application/json", "Content-Type": "application/json", "Plug-RPC": "1" },
      body: JSON.stringify({ jsonrpc: "2.0", id, method, params: input }),
    });
    const raw = await response.text();
    signal.throwIfAborted();
    if (raw.length > 2 * 1024 * 1024) throw new RPCError("The response exceeds this page's read limit.", "response_too_large");
    let envelope;
    try { envelope = parseJSON(raw); } catch (error) {
      if (error instanceof RPCError) throw error;
      throw new RPCError("The RPC endpoint returned invalid JSON.", "invalid_response");
    }
    if (envelope && envelope.error) {
      const data = envelope.error.data || {};
      throw new RPCError(data.hint || envelope.error.message || "The server refused this read.", data.code || "http_" + response.status);
    }
    if (!response.ok || !envelope || envelope.jsonrpc !== "2.0" || envelope.id !== id || !Object.hasOwn(envelope, "result")) throw new RPCError("The RPC response does not match this read.", "invalid_response");
    return envelope.result;
  }
  async function configure(signal) {
    const response = await fetch("./config.json", { credentials: "omit", mode: "same-origin", redirect: "error", cache: "no-store", signal });
    if (!response.ok) throw new RPCError("The UI's server composition is unavailable.", "unconfigured");
    state.config = await response.json();
    if (!state.config || !Number.isInteger(state.config.maxRows) || state.config.maxRows < 1 || state.config.maxRows > 100) throw new RPCError("The row budget is not configured.", "unconfigured");
    const hello = await rpc("server.hello", { clientVersion: "plug-web-ui/0.1", protocolVersion: "1" }, signal);
    if (hello.authMode !== "anonymous") throw new RPCError("Interactive browser sessions are not configured in this UI.", "sessions_unconfigured");
    const discovery = await rpc("rpc.methods", {}, signal);
    if (!Array.isArray(discovery.methods)) throw new RPCError("Server capabilities are unreadable.", "invalid_response");
    state.hello = hello; state.methods = new Set(discovery.methods);
    text($("budget-number"), state.config.maxRows); text($("next-budget-number"), state.config.maxRows);
    text($("footer-project"), "Project " + state.config.project); text($("footer-auth"), "Anonymous · loopback only");
    text($("capability-note"), "Only the rows in this page are compared with your previous read. Continue the snapshot to inspect the next page.");
    text($("home-boundary"), "Refresh reads a new snapshot. Live watches are unavailable in this UI; rows stay in server order.");
    renderCatalog(state.config.catalog || {});
    renderFrames(Array.isArray(state.config.frames) ? state.config.frames : []);
  }
  const bridgeDeniedMethods = new Set(["plugin.plan", "plugin.apply", "ontology.apply"]);
  const bridgeDeniedPrefixes = ["auth.", "network.", "http.", "secret."];
  const bridgeDeniedFields = new Set(["authorization", "cookie", "credentials", "token", "access_token", "secret", "url", "endpoint", "origin"]);
  function bridgeMethodDenied(method) { return bridgeDeniedMethods.has(method) || bridgeDeniedPrefixes.some((prefix) => method.startsWith(prefix)); }
  function bridgeFieldDenied(value) {
    if (!value || typeof value !== "object") return false;
    if (Array.isArray(value)) return value.some(bridgeFieldDenied);
    return Object.entries(value).some(([key, child]) => bridgeDeniedFields.has(key.toLowerCase()) || bridgeFieldDenied(child));
  }
  function bridgeResponse(source, response) { source.postMessage(response, "*"); }
  function bridgeFailure(source, id, code, detail) {
    bridgeResponse(source, { jsonrpc: "2.0", id: id == null ? null : id, error: { code: -32000, message: detail, data: { code } } });
  }
  function installBridge(frame, descriptor) {
    const methods = new Set(Array.isArray(descriptor.methods) ? descriptor.methods : []);
    const project = typeof descriptor.project === "string" ? descriptor.project : state.config.project;
    const callTimes = [];
    const onMessage = async (event) => {
      // Sandboxed frames have origin "null". The WindowProxy is the identity;
      // origin alone cannot distinguish two opaque plugin frames.
      if (event.source !== frame.contentWindow || event.origin !== "null") return;
      let size;
      try { size = JSON.stringify(event.data).length; } catch (_) { bridgeFailure(event.source, null, "invalid_message", "Bridge message is not serializable."); return; }
      if (size > 256 * 1024) { bridgeFailure(event.source, event.data && event.data.id, "message_too_large", "Bridge message exceeds the framing limit."); return; }
      const now = Date.now();
      while (callTimes.length && now - callTimes[0] >= 1000) callTimes.shift();
      if (callTimes.length >= 20) { bridgeFailure(event.source, event.data && event.data.id, "rate_limited", "Bridge call rate exceeds the per-frame limit."); return; }
      callTimes.push(now);
      const request = event.data;
      if (!request || request.jsonrpc !== "2.0" || (typeof request.id !== "string" && !(typeof request.id === "number" && Number.isFinite(request.id))) || typeof request.method !== "string") {
        bridgeFailure(event.source, request && request.id, "invalid_message", "Bridge request envelope is invalid."); return;
      }
      if (!methods.has(request.method) || bridgeMethodDenied(request.method)) {
        bridgeFailure(event.source, request.id, "method_not_granted", "Bridge method is not granted."); return;
      }
      const params = request.params == null ? {} : request.params;
      if (!params || typeof params !== "object" || Array.isArray(params) || bridgeFieldDenied(params)) {
        bridgeFailure(event.source, request.id, "bridge_field_denied", "Bridge params contain a credential or egress field."); return;
      }
      const forwarded = Object.assign({}, params, { _meta: { project } });
      try {
        const result = await rpc(request.method, forwarded, new AbortController().signal, project);
        bridgeResponse(event.source, { jsonrpc: "2.0", id: request.id, result });
      } catch (error) {
        bridgeFailure(event.source, request.id, error && error.code || "bridge_refused", failure(error));
      }
    };
    window.addEventListener("message", onMessage);
    state.bridges.push(() => window.removeEventListener("message", onMessage));
  }
  function renderCatalog(catalog) {
    const node = $("catalog-entries");
    if (!node) return;
    clear(node);
    const entries = [];
    (Array.isArray(catalog.panels) ? catalog.panels : []).forEach((entry) => entries.push({ kind: "panel", title: entry.title, detail: entry.view + " view" }));
    (Array.isArray(catalog.columns) ? catalog.columns : []).forEach((entry) => entries.push({ kind: "column", title: entry.title, detail: entry.field + " · " + entry.view + " view" }));
    (Array.isArray(catalog.buttons) ? catalog.buttons : []).forEach((entry) => entries.push({ kind: "button", title: entry.label, detail: entry.method }));
    if (!entries.length) { message(node, "empty", "No additions configured.", "This page uses only its built-in views."); return; }
    entries.forEach((entry) => {
      const card = document.createElement("article"); card.className = "catalog-entry"; card.dataset.catalogKind = entry.kind;
      const kind = document.createElement("div"); kind.className = "catalog-entry-kind"; text(kind, entry.kind);
      const title = document.createElement("div"); title.className = "catalog-entry-title"; text(title, entry.title);
      const detail = document.createElement("div"); detail.className = "catalog-entry-detail"; text(detail, entry.detail);
      card.append(kind, title, detail); node.appendChild(card);
    });
  }
  function renderFrames(frames) {
    const node = $("plugin-frames");
    if (!node) return;
    clear(node); node.hidden = true;
    frames.forEach((descriptor) => {
      if (!descriptor || typeof descriptor.id !== "string" || typeof descriptor.src !== "string" || bridgeFieldDenied(descriptor)) return;
      let source;
      try { source = new URL(descriptor.src, location.href); } catch (_) { return; }
      if (source.origin !== location.origin || !source.pathname.includes("/frames/")) return;
      const article = document.createElement("article"); article.className = "plugin-frame";
      const heading = document.createElement("div"); heading.className = "plugin-frame-heading"; text(heading, descriptor.title || descriptor.id);
      const frame = document.createElement("iframe"); frame.title = descriptor.title || descriptor.id; frame.setAttribute("sandbox", "allow-scripts"); frame.setAttribute("referrerpolicy", "no-referrer"); frame.src = source.href;
      article.append(heading, frame); node.appendChild(article); node.hidden = false; installBridge(frame, descriptor);
    });
  }
  function requireMethod(name) {
    if (!state.methods.has(name)) throw new RPCError("This server does not expose " + name + ".", "method_not_found");
  }
  function bounded(result) {
    if (!result || !Array.isArray(result.items) || result.items.length > state.config.maxRows) throw new RPCError("The server result exceeds this page's row budget.", "response_exceeds_budget");
    if (result.groups && result.groups.length) throw new RPCError("Grouped view results are not configured in this UI.", "unsupported_result");
    asOfParams(result.asOf);
    return result;
  }
  function rowTitle(item) { return item && item.fields && typeof item.fields.title === "string" && item.fields.title.trim() ? item.fields.title : String(item.id); }
  function renderRows(node, items, emptyTitle, emptyDetail) {
    clear(node);
    if (!items.length) { message(node, "empty", emptyTitle, emptyDetail); return; }
    items.forEach((item) => {
      const row = document.createElement("article"); row.className = "row" + (count(item.hiddenBlockers) ? " is-blocked" : "");
      const content = document.createElement("div"); content.className = "row-content";
      const title = document.createElement("div"); title.className = "row-title"; text(title, rowTitle(item));
      const meta = document.createElement("div"); meta.className = "row-meta"; text(meta, [item.type, item.status, item.id].filter(Boolean).join(" · "));
      content.append(title, meta);
      if (count(item.hiddenBlockers)) {
        const badge = document.createElement("span"); badge.className = "row-badge"; text(badge, item.hiddenBlockers + " unreadable blockers"); content.appendChild(badge);
      }
      const revision = document.createElement("span"); revision.className = "row-revision"; text(revision, "REV " + item.revision);
      row.append(content, revision); node.appendChild(row);
    });
  }
  function continuation(node, cursor, detail) {
    clear(node); node.hidden = !cursor;
    if (!cursor) return;
    const copy = document.createElement("span"); text(copy, detail + " More rows exist; their count is not supplied by the server. ");
    const button = document.createElement("button"); button.className = "button button-quiet"; button.type = "button"; text(button, "Next page");
    button.addEventListener("click", () => load(true)); node.append(copy, button);
  }
  // A bounded map records only rows actually displayed, scoped to endpoint,
  // project, and store incarnation. A page cut never marks unseen rows read.
  function seenKey(at) { return "plug:seen:" + JSON.stringify([state.config.rpcPath, state.config.project, at.incarnation]); }
  function readSeen(at) {
    try {
      const value = JSON.parse(localStorage.getItem(seenKey(at)) || "[]");
      return new Map(Array.isArray(value) ? value.slice(-1000).filter((x) => Array.isArray(x) && typeof x[0] === "string" && integer(x[1])) : []);
    } catch (_) { return new Map(); }
  }
  function markSeen(rows, seen, at) {
    rows.forEach((item) => { seen.delete(item.id); seen.set(item.id, String(item.revision)); });
    try { localStorage.setItem(seenKey(at), JSON.stringify(Array.from(seen.entries()).slice(-1000))); } catch (_) { /* private/disabled storage remains usable */ }
  }
  async function home(nextPage, signal) {
    requireMethod("bead.ready"); requireMethod("history.query");
    const ready = bounded(await rpc("bead.ready", { limit: state.config.maxRows }, signal));
    const params = { asOf: asOfParams(nextPage && state.home ? state.home.asOf : ready.asOf), limit: state.config.maxRows };
    if (nextPage && state.home && state.home.nextCursor) params.cursor = state.home.nextCursor;
    const snapshot = bounded(await rpc("history.query", params, signal));
    const seen = readSeen(snapshot.asOf);
    const changed = snapshot.items.filter((item) => !seen.has(item.id) || BigInt(item.revision) > BigInt(seen.get(item.id)));
    state.home = snapshot;
    renderRows($("changed-rows"), changed, "Nothing changed in this page.", "No displayed row moved after your previous read. Continue to inspect more rows.");
    continuation($("changed-more"), snapshot.nextCursor, snapshot.items.length + " stored rows inspected in this page.");
    text($("changed-count"), changed.length); text($("changed-caption"), "Changed in this page");
    text($("ready-count"), ready.items.length); text($("ready-caption"), ready.nextCursor ? "First page · more rows exist" : "Rows in the current ready read");
    text($("revision-count"), snapshot.asOf.revision); text($("revision-caption"), "Pinned snapshot · not a live watch");
    text($("snapshot-state"), "Snapshot rev " + snapshot.asOf.revision);
    text($("presence-count"), "—"); text($("presence-caption"), state.methods.has("presence.list") ? "Open Presence to read sessions" : "Unavailable on this server");
    markSeen(snapshot.items, seen, snapshot.asOf);
  }
  async function ready(nextPage, signal) {
    requireMethod("bead.ready");
    const params = { limit: state.config.maxRows };
    if (nextPage && state.next && state.next.nextCursor) params.cursor = state.next.nextCursor;
    state.next = bounded(await rpc("bead.ready", params, signal));
    renderRows($("next-rows"), state.next.items, "The ready queue is empty.", "No ready row was returned for this project.");
    continuation($("next-more"), state.next.nextCursor, state.next.items.length + " ready rows shown.");
    text($("snapshot-state"), "Ready read rev " + state.next.asOf.revision);
  }
  async function blocked(nextPage, signal) {
    if (!state.methods.has("view.run")) { message($("blocked-state"), "unavailable", "Blocked view unavailable.", "This server does not expose a bounded blocked view."); return; }
    if (!state.config.blockedViewDefinition && !state.config.blockedViewId) { message($("blocked-state"), "unavailable", "No blocked view configured.", "The server operator must select a supported blocked view before it can be shown."); return; }
    const params = state.config.blockedViewDefinition ? { definition: state.config.blockedViewDefinition } : { viewId: state.config.blockedViewId };
    if (nextPage && state.blocked && state.blocked.nextCursor) { params.cursor = state.blocked.nextCursor; params.asOf = asOfParams(state.blocked.asOf); }
    state.blocked = bounded(await rpc("view.run", params, signal));
    renderRows($("blocked-state"), state.blocked.items, "No blocked rows in this view.", "The server returned an empty authorized view.");
    continuation($("blocked-more"), state.blocked.nextCursor, state.blocked.items.length + " blocked rows shown.");
  }
  async function presence(signal) {
    if (!state.methods.has("presence.list")) { message($("presence-rows"), "unavailable", "Presence unavailable.", "This server does not expose live presence."); return; }
    const result = await rpc("presence.list", { project: state.config.project }, signal);
    if (!Array.isArray(result.presence)) throw new RPCError("The presence response is unreadable.", "invalid_response");
    clear($("presence-rows"));
    if (!result.presence.length) { message($("presence-rows"), "empty", "No active sessions.", "Presence is ephemeral; no session is visible in this project."); return; }
    result.presence.slice(0, 5).forEach((entry) => {
      const row = document.createElement("div"); row.className = "presence-row";
      const principal = String(entry.onBehalfOf || entry.principal.id);
      const initial = document.createElement("span"); initial.className = "presence-initial"; text(initial, principal.slice(0, 1).toUpperCase());
      const content = document.createElement("div");
      const name = document.createElement("div"); name.className = "presence-name"; text(name, principal);
      const focus = document.createElement("div"); focus.className = "presence-focus"; text(focus, entry.focus || "No focus shared"); content.append(name, focus);
      const scope = document.createElement("span"); scope.className = "presence-scope"; text(scope, (entry.workingOn || []).length + " claims");
      row.append(initial, content, scope); $("presence-rows").appendChild(row);
    });
    if (result.presence.length > 5) {
      const remainder = document.createElement("p"); remainder.className = "elision"; text(remainder, (result.presence.length - 5) + " more visible sessions are hidden to keep this view compact."); $("presence-rows").appendChild(remainder);
    }
  }
  async function explain(signal) {
    const node = $("explain-state");
    if (!state.methods.has("view.explain") || !state.config.blockedViewId) { message(node, "unavailable", "Explanation unavailable.", "A server-backed saved view and its explanation method are not configured."); return; }
    const itemId = $("explain-id").value.trim();
    if (!itemId) { text(node, "Enter an item ID to request its explanation."); return; }
    const result = await rpc("view.explain", { viewId: state.config.blockedViewId, itemId }, signal);
    clear(node);
    const summary = document.createElement("p"); text(summary, result.included ? "Included in the view." : "Outside the current view."); node.appendChild(summary);
    const reasons = document.createElement("ul"); reasons.className = "explain-reasons";
    (result.reasons || []).forEach((reason) => { const li = document.createElement("li"); text(li, reason); reasons.appendChild(li); }); node.appendChild(reasons);
    if (count(result.hiddenBlockers)) { const copy = document.createElement("p"); text(copy, result.hiddenBlockers + " blockers cannot be read with current permissions."); node.appendChild(copy); }
  }
  function target(view) { return $({ home: "changed-rows", next: "next-rows", blocked: "blocked-state", presence: "presence-rows", explain: "explain-state" }[view]); }
  async function load(nextPage) {
    if (state.work) state.work.controller.abort();
    const work = { controller: new AbortController(), timedOut: false };
    const view = state.activeView; state.work = work;
    const timer = setTimeout(() => { work.timedOut = true; work.controller.abort(); }, 15000);
    $("cancel").hidden = false; $("refresh").disabled = true;
    text($("snapshot-state"), "Waiting for a completed read");
    const more = $({ home: "changed-more", next: "next-more", blocked: "blocked-more" }[view]);
    if (more) { clear(more); more.hidden = true; }
    setConnection("loading", "Loading"); setMessage("Reading the server…");
    message(target(view), "loading", "Loading…", "Waiting for the bounded server read.");
    try {
      if (!state.hello) await configure(work.controller.signal);
      if (view === "home") await home(nextPage, work.controller.signal);
      else if (view === "next") await ready(nextPage, work.controller.signal);
      else if (view === "blocked") await blocked(nextPage, work.controller.signal);
      else if (view === "presence") await presence(work.controller.signal);
      else if (view === "explain") await explain(work.controller.signal);
      if (state.work !== work) return;
      setConnection("ok", "Connected"); setMessage("");
    } catch (error) {
      if (state.work !== work) return;
      if (work.controller.signal.aborted) {
        const detail = work.timedOut ? "The server read timed out. Refresh to try again." : "Request cancelled; nothing changed.";
        setConnection("loading", work.timedOut ? "Timed out" : "Cancelled"); setMessage(detail, "cancelled"); message(target(view), "cancelled", work.timedOut ? "Timed out." : "Read cancelled.", detail);
      } else { setConnection("error", "Unavailable"); setMessage(failure(error), "error"); message(target(view), "error", "This read was refused.", failure(error)); }
    } finally {
      clearTimeout(timer);
      if (state.work === work) { state.work = null; $("cancel").hidden = true; $("refresh").disabled = false; }
    }
  }
  function selectView(name) {
    state.activeView = name;
    document.querySelectorAll(".tab").forEach((tab) => { const active = tab.dataset.view === name; tab.classList.toggle("is-active", active); tab.setAttribute("aria-selected", String(active)); });
    document.querySelectorAll(".view-panel").forEach((panel) => { panel.hidden = panel.dataset.panel !== name; });
    load(false);
  }
  $("refresh").addEventListener("click", () => load(false));
  $("cancel").addEventListener("click", () => { if (state.work) state.work.controller.abort(); });
  $("explain-form").addEventListener("submit", (event) => { event.preventDefault(); load(false); });
  document.querySelectorAll(".tab").forEach((tab) => tab.addEventListener("click", () => selectView(tab.dataset.view)));
  const requested = new URLSearchParams(location.search).get("view");
  selectView(["next", "blocked", "explain", "presence"].includes(requested) ? requested : "home");
}());
