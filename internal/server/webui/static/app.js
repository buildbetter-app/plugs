(function () {
  "use strict";

  // A supplied credential is deliberately held only in this page closure.
  // Never persist it, expose it to plugin frames, or send it with static assets.
  let accessToken = "", credentialGeneration = 0;
  const authenticatedSeen = new Map();
  const state = { config: null, hello: null, methods: new Set(), activeView: "home", work: null, live: null, sequence: 0, home: null, next: null, blocked: null, bridges: [] };
  const $ = (id) => document.getElementById(id);
  const text = (node, value) => { node.textContent = value == null ? "" : String(value); };
  const clear = (node) => node.replaceChildren();
  const integer = (value) => /^(0|[1-9][0-9]*)$/.test(String(value));
  const count = (value) => Number.isSafeInteger(value) && value >= 0 ? value : 0;

  class RPCError extends Error {
    constructor(message, code) { super(message); this.name = "RPCError"; this.code = code || "unavailable"; }
  }
  function failure(error) { return error && error.code ? error.code + ": " + error.message : (error && error.message || "The server could not complete this read."); }
  function authenticationFailure(error, generation = credentialGeneration) {
    if (generation !== credentialGeneration || !error || !["unauthenticated", "token_expired"].includes(error.code)) return false;
    forgetIdentity();
    watchStatus("Live changes stopped · sign-in required.");
    setConnection("error", "Sign-in required");
    setMessage("Sign in with a current key for this server.", "error");
    return true;
  }
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
      if ((key === "revision" || key === "ontologyRevision" || key === "branch") && typeof value === "number") {
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
  function authenticationContext() {
    return { token: accessToken, generation: credentialGeneration, rpcPath: state.config.rpcPath, project: state.config.project };
  }
  async function rpc(method, params, signal, requestedProject, session, authentication = authenticationContext()) {
    const cleanup = method === "event.unsubscribe" || method === "session.close";
    // A retired identity may finish only its own same-origin allocation cleanup.
    // Never use its captured key to open another watch or perform a fresh read.
    if (authentication.generation !== credentialGeneration && !cleanup) throw new DOMException("Read cancelled", "AbortError");
    const input = Object.assign({}, params || {});
    const project = typeof requestedProject === "string" && requestedProject ? requestedProject : authentication.project;
    if (method !== "server.hello") input._meta = { project };
    const id = "web-ui-" + (++state.sequence);
    const response = await fetch(authentication.rpcPath, {
      method: "POST", mode: "same-origin", credentials: "omit", redirect: "error", cache: "no-store", signal,
      keepalive: cleanup,
      headers: Object.assign({ "Accept": "application/json", "Content-Type": "application/json", "Plug-RPC": "1" }, authentication.token ? { "Authorization": "Bearer " + authentication.token } : {}, session ? { "Plug-Session": session } : {}),
      body: JSON.stringify({ jsonrpc: "2.0", id, method, params: input }),
    });
    const raw = await response.text();
    signal.throwIfAborted();
    if (raw.length > 2 * 1024 * 1024) throw new RPCError("The response exceeds this page's read limit.", "response_too_large");
    if (response.status === 401) throw new RPCError("Sign in with a current key for this server.", "unauthenticated");
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
    if (!["anonymous", "authenticated"].includes(hello.authMode)) throw new RPCError("The server authentication mode is unavailable.", "unavailable");
    if (hello.authMode === "authenticated" && !accessToken) throw new RPCError("Sign in with a current key for this server.", "unauthenticated");
    const discovery = await rpc("rpc.methods", {}, signal);
    if (!Array.isArray(discovery.methods)) throw new RPCError("Server capabilities are unreadable.", "invalid_response");
    state.hello = hello; state.methods = new Set(discovery.methods);
    text($("budget-number"), state.config.maxRows); text($("next-budget-number"), state.config.maxRows);
    text($("footer-project"), "Project " + state.config.project); text($("footer-auth"), hello.authMode === "authenticated" ? "Authenticated · bearer credential" : "Anonymous · loopback only");
    $("authentication").hidden = hello.authMode !== "authenticated";
    $("sign-out").hidden = hello.authMode !== "authenticated";
    if (hello.authMode === "authenticated") text($("auth-state"), "Connected. The key stays in this tab until sign-out or reload.");
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
  // A frame grant is the authority for every project-bearing value in a call.
  // Check the caller's complete structured-clone payload before adding the
  // host-owned _meta so top-level, nested, and list operations cannot select a
  // different project through a method binding.
  function bridgeProjectDenied(value, expected) {
    if (Array.isArray(value)) return value.some((child) => bridgeProjectDenied(child, expected));
    if (!value || typeof value !== "object") return false;
    return Object.entries(value).some(([key, child]) => {
      if (key === "project") return typeof child !== "string" || child !== expected;
      if (key === "projects") return !Array.isArray(child) || Array.from(child).some((project) => typeof project !== "string" || project !== expected);
      return bridgeProjectDenied(child, expected);
    });
  }
  function bridgeResponse(source, response) { source.postMessage(response, "*"); }
  function bridgeFailure(source, id, code, detail) {
    bridgeResponse(source, { jsonrpc: "2.0", id: id == null ? null : id, error: { code: -32000, message: detail, data: { code } } });
  }
  function installBridge(frame, descriptor) {
    const methods = new Set(Array.isArray(descriptor.methods) ? descriptor.methods : []);
    const project = typeof descriptor.project === "string" ? descriptor.project : state.config.project;
    const callTimes = [];
    const pending = new Set();
    let active = true, loads = 0;
    const revoke = () => {
      if (!active) return;
      active = false;
      frame.dataset.bridgeState = "revoked";
      pending.forEach((controller) => controller.abort());
      pending.clear();
      window.removeEventListener("message", onMessage);
      frame.removeEventListener("load", onLoad);
    };
    const onLoad = () => { if (++loads > 1) revoke(); };
    const onMessage = async (event) => {
      // Bind the trusted same-origin wrapper, never an untrusted document's
      // reusable WindowProxy. Its child has an opaque sandbox and a separate
      // exact-entry frame-src policy which contains navigation before scripts.
      if (!active || event.source !== frame.contentWindow || event.origin !== location.origin) return;
      if (event.data?.plugFrameLifecycle === "revoked" && !Object.hasOwn(event.data, "jsonrpc")) { revoke(); return; }
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
      if (bridgeProjectDenied(params, project)) {
        bridgeFailure(event.source, request.id, "scope_denied", "Bridge params are outside the granted project."); return;
      }
      const forwarded = Object.assign({}, params, { _meta: { project } });
      const controller = new AbortController();
      pending.add(controller);
      try {
        const result = await rpc(request.method, forwarded, controller.signal, project);
        if (active) bridgeResponse(event.source, { jsonrpc: "2.0", id: request.id, result });
      } catch (error) {
        if (active && !authenticationFailure(error)) bridgeFailure(event.source, request.id, error && error.code || "bridge_refused", failure(error));
      } finally {
        pending.delete(controller);
      }
    };
    window.addEventListener("message", onMessage);
    frame.addEventListener("load", onLoad);
    state.bridges.push(revoke);
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
    state.bridges.forEach((revoke) => revoke()); state.bridges = [];
    clear(node); node.hidden = true;
    frames.forEach((descriptor) => {
      if (!descriptor || typeof descriptor.id !== "string" || typeof descriptor.src !== "string" || bridgeFieldDenied(descriptor)) return;
      let source;
      try { source = new URL(descriptor.src, location.href); } catch (_) { return; }
      const expected = new URL("./frame-hosts/" + encodeURIComponent(descriptor.id) + "/index.html", location.href);
      if (source.href !== expected.href) return;
      const article = document.createElement("article"); article.className = "plugin-frame";
      const heading = document.createElement("div"); heading.className = "plugin-frame-heading"; text(heading, descriptor.title || descriptor.id);
      const frame = document.createElement("iframe"); frame.title = descriptor.title || descriptor.id; frame.setAttribute("referrerpolicy", "no-referrer");
      installBridge(frame, descriptor); frame.src = source.href;
      article.append(heading, frame); node.appendChild(article); node.hidden = false;
    });
  }
  function requireMethod(name) {
    if (!state.methods.has(name)) throw new RPCError("This server does not expose " + name + ".", "method_not_found");
  }
  function bounded(result) {
    if (!result || !Array.isArray(result.items) || result.items.length > state.config.maxRows) throw new RPCError("The server result exceeds this page's row budget.", "response_exceeds_budget");
    const ids = new Set();
    result.items.forEach((item) => {
      if (!item || typeof item.id !== "string" || !item.id || !integer(item.revision) || ids.has(item.id) || (item.project && item.project !== state.config.project)) throw new RPCError("The row identity or project is invalid.", "invalid_response");
      ids.add(item.id);
    });
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
    if (state.hello && state.hello.authMode === "authenticated") return new Map(authenticatedSeen.get(seenKey(at)) || []);
    try {
      const value = JSON.parse(localStorage.getItem(seenKey(at)) || "[]");
      return new Map(Array.isArray(value) ? value.slice(-1000).filter((x) => Array.isArray(x) && typeof x[0] === "string" && integer(x[1])) : []);
    } catch (_) { return new Map(); }
  }
  function markSeen(rows, seen, at) {
    rows.forEach((item) => { seen.delete(item.id); seen.set(item.id, String(item.revision)); });
    if (state.hello && state.hello.authMode === "authenticated") {
      authenticatedSeen.set(seenKey(at), Array.from(seen.entries()).slice(-1000)); return;
    }
    try { localStorage.setItem(seenKey(at), JSON.stringify(Array.from(seen.entries()).slice(-1000))); } catch (_) { /* private/disabled storage remains usable */ }
  }
  async function home(nextPage, signal) {
    if (state.methods.has("view.run") || state.config.homeViewId) {
      requireMethod("view.run");
      if (!nextPage) await startWatch(signal);
      else watchStatus("Snapshot page · live changes stopped.");
      const params = homeParams();
      if (nextPage && state.home && state.home.nextCursor) { params.cursor = state.home.nextCursor; params.asOf = asOfParams(state.home.asOf); }
      renderHome(bounded(await rpc("view.run", params, signal)), false);
      text($("home-rows-heading"), "Stored rows in this view");
      text($("home-description"), "The current bounded view page. Changed counts compare rows you have actually seen; live changes keep this first page current.");
      text($("home-boundary"), "Stored rows come from the configured view. Live changes reread its first bounded page; next pages keep their pinned snapshot.");
      return;
    }
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

  function homeParams() {
    return state.config.homeViewId ? { viewId: state.config.homeViewId } : {
      definition: { filter: {}, fields: ["id", "type", "project", "status", "revision", "fields.title"], sort: ["id"], budgetTokens: state.config.maxRows },
    };
  }
  function renderHome(snapshot, stable) {
    // Retain the relative positions of surviving displayed rows. New members
    // follow in server order; never append a second page or exceed its budget.
    if (stable && state.home) {
      const rows = new Map(snapshot.items.map((item) => [item.id, item]));
      const previous = state.home.items.filter((item) => rows.has(item.id)).map((item) => rows.get(item.id));
      const ids = new Set(previous.map((item) => item.id));
      snapshot.items = previous.concat(snapshot.items.filter((item) => !ids.has(item.id)));
    }
    const seen = readSeen(snapshot.asOf);
    const changed = snapshot.items.filter((item) => !seen.has(item.id) || BigInt(item.revision) > BigInt(seen.get(item.id)));
    state.home = snapshot;
    renderRows($("changed-rows"), snapshot.items, "No stored rows in this view.", "The server returned an empty authorized view.");
    continuation($("changed-more"), snapshot.nextCursor, snapshot.items.length + " stored rows in this view page.");
    text($("changed-count"), changed.length); text($("changed-caption"), "Changed since the previous read");
    text($("ready-count"), "—"); text($("ready-caption"), "Open What next for the ready queue");
    text($("revision-count"), snapshot.asOf.revision); text($("revision-caption"), "Stored view snapshot");
    text($("snapshot-state"), "View rev " + snapshot.asOf.revision);
    text($("presence-caption"), state.methods.has("presence.list") ? "Open Presence to read sessions" : "Unavailable on this server");
    markSeen(snapshot.items, seen, snapshot.asOf);
  }
  function watchStatus(label) { text($("watch-state"), label); $("stop-watch").hidden = !state.live; }
  async function releaseWatch(live, closeSession = true) {
    // Opening calls retain their short timeout even after navigation, so a
    // late server allocation is read and released using its originating key.
    const subscription = live.subscription, session = live.session;
    live.subscription = null; if (closeSession) live.session = null;
    // Dispatch both keepalive requests before awaiting either, including on
    // pagehide where this document may disappear before a promise resumes.
    const results = await Promise.allSettled([
      ...(subscription ? [rpc("event.unsubscribe", { subscriptionId: subscription }, AbortSignal.timeout(3000), null, session, live.authentication)] : []),
      ...(closeSession && session ? [rpc("session.close", { sessionId: session }, AbortSignal.timeout(3000), null, session, live.authentication)] : []),
    ]);
    for (const result of results) {
      if (result.status === "rejected" && authenticationFailure(result.reason, live.authentication.generation)) break;
    }
  }
  function stopWatch() {
    const live = state.live;
    if (!live) return;
    state.live = null; clearTimeout(live.openTimer); live.controller.abort();
    void releaseWatch(live); $("stop-watch").hidden = true;
  }
  function liveFailure(live, error) {
    if (state.live !== live) return;
    if (authenticationFailure(error)) return;
    if (state.work) { state.work.liveFailure = true; state.work.controller.abort(); }
    stopWatch(); state.home = null;
    clear($("changed-more")); $("changed-more").hidden = true;
    text($("changed-count"), "—"); text($("revision-count"), "—"); text($("snapshot-state"), "Refresh required");
    watchStatus("Live changes refused · refresh required.");
    setConnection("error", "Unavailable"); setMessage(failure(error), "error");
    message($("changed-rows"), "error", "Live read stopped.", failure(error));
  }
  async function startWatch(signal) {
    if (!state.config.homeViewId || !state.config.watchPath || !["view.watch", "event.unsubscribe", "session.open", "session.close"].every((method) => state.methods.has(method))) {
      watchStatus("Live changes unavailable on this server."); return;
    }
    const live = { controller: new AbortController(), authentication: authenticationContext(), watchPath: state.config.watchPath, session: null, subscription: null, cursor: null, dirty: false, busy: false };
    state.live = live; watchStatus("Opening live changes…");
    let cancelled;
    const aborted = new Promise((_, reject) => {
      cancelled = () => { if (state.live === live) stopWatch(); reject(new DOMException("Read cancelled", "AbortError")); };
      signal.addEventListener("abort", cancelled, { once: true });
    });
    const opening = (async () => { try {
      const session = await rpc("session.open", {}, AbortSignal.timeout(15000), null, null, live.authentication);
      if (!session || typeof session.sessionId !== "string" || !session.sessionId) throw new RPCError("No collaboration lifetime was returned.", "invalid_response");
      live.session = session.sessionId;
      if (state.live !== live) { await releaseWatch(live); signal.throwIfAborted(); return; }
      let from;
      // An omitted-from open can encounter the stored ontology seed. Its first
      // terminal frame supplies the opaque reopen marker; never infer one from
      // a view revision, and never automatically recover a later boundary.
      for (let attempt = 0; attempt < 2; attempt++) {
        const params = { viewId: state.config.homeViewId };
        if (from) params.from = from;
        const opened = await rpc("view.watch", params, AbortSignal.timeout(15000), null, live.session, live.authentication);
        if (!opened || typeof opened.subscriptionId !== "string" || !opened.subscriptionId || typeof opened.cursor !== "string" || !opened.cursor) throw new RPCError("The watch checkpoint is unreadable.", "invalid_response");
        live.subscription = opened.subscriptionId; live.cursor = opened.cursor;
        if (state.live !== live) { await releaseWatch(live); signal.throwIfAborted(); return; }
        let received, refused;
        const firstFrame = new Promise((resolve, reject) => { received = resolve; refused = reject; });
        live.openTimer = setTimeout(() => liveFailure(live, new RPCError("The live stream did not open before its deadline.", "watch_timeout")), 15000);
        const streaming = streamWatch(live, received, attempt === 0);
        void streaming.catch((error) => { refused(error); liveFailure(live, error); });
        const bootstrap = await firstFrame;
        if (!bootstrap) break;
        // Await reader cancellation and release only the old subscription,
        // checking auth failures while retaining the session for the reopen.
        await streaming;
        await releaseWatch(live, false);
        if (state.live !== live) { signal.throwIfAborted(); return; }
        from = bootstrap;
      }
      // Streaming is attached before the snapshot. Racing deltas remain dirty
      // until the normal first read finishes, preserving snapshot continuity.
    } catch (error) {
      if (state.live === live) stopWatch();
      await releaseWatch(live);
      throw error;
    } })();
    try { if (signal.aborted) cancelled(); await Promise.race([opening, aborted]); }
    finally { signal.removeEventListener("abort", cancelled); }
  }
  function inspectFrame(live, frame, first) {
    if (!frame || frame.subscriptionId !== live.subscription || typeof frame.cursor !== "string" || !frame.cursor || !Array.isArray(frame.events)) throw new RPCError("The live frame does not match this view.", "invalid_response");
    if (first && frame.cursor !== live.cursor) throw new RPCError("The opening live checkpoint changed.", "invalid_response");
    const inspect = (event) => {
      if (!event || typeof event.type !== "string") throw new RPCError("The live event is unreadable.", "invalid_response");
      if (["watch.reset", "event.gap", "visibility.lost"].includes(event.type)) throw new RPCError("The live view needs a fresh snapshot (" + event.type + ").", "watch_reset");
      if (event.type === "watch.delta") {
        const details = event.details;
        if (!details || details.subscriptionId !== live.subscription || details.fromCursor !== live.cursor || details.toCursor !== frame.cursor || !Array.isArray(details.events)) throw new RPCError("The live cursor chain changed.", "invalid_response");
        details.events.forEach((child) => { if (child.type === "watch.delta") throw new RPCError("Nested live frames are invalid.", "invalid_response"); inspect(child); });
      }
    };
    frame.events.forEach(inspect);
    live.cursor = frame.cursor;
    if (!first) live.dirty = true;
  }
  function bootstrapCursor(live, frame) {
    if (!frame || frame.subscriptionId !== live.subscription || frame.cursor !== live.cursor || !Array.isArray(frame.events) || frame.events.length !== 1) return null;
    const event = frame.events[0], details = event && event.details;
    return event && event.type === "watch.reset" && event.cursor === frame.cursor && details && details.reason === "ontology_changed" && details.subscriptionId === live.subscription && details.snapshotCursor === frame.cursor ? frame.cursor : null;
  }
  async function streamWatch(live, received, allowBootstrap) {
    if (state.live !== live || live.authentication.generation !== credentialGeneration) throw new DOMException("Read cancelled", "AbortError");
    const response = await fetch(live.watchPath, {
      method: "POST", mode: "same-origin", credentials: "omit", redirect: "error", cache: "no-store", signal: live.controller.signal,
      headers: Object.assign({ "Content-Type": "application/json", "Accept": "application/x-ndjson", "Plug-RPC": "1", "Plug-Session": live.session }, live.authentication.token ? { "Authorization": "Bearer " + live.authentication.token } : {}),
      body: JSON.stringify({ subscriptionId: live.subscription }),
    });
    if (response.status === 401) throw new RPCError("Sign in with a current key for this server.", "unauthenticated");
    if (!response.ok || !response.headers.get("Content-Type")?.startsWith("application/x-ndjson") || !response.body) throw new RPCError("The server refused the live stream.", "watch_unavailable");
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "", first = true;
    try {
      while (state.live === live) {
        const { value, done } = await reader.read();
        if (done) throw new RPCError("The live stream ended. Refresh to reconnect.", "watch_closed");
        buffer += decoder.decode(value, { stream: true });
        let end;
        while ((end = buffer.indexOf("\n")) !== -1) {
          if (end > 2 * 1024 * 1024) throw new RPCError("The live frame exceeds the read limit.", "response_too_large");
          const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
          const frame = parseJSON(line);
          const bootstrap = first && allowBootstrap ? bootstrapCursor(live, frame) : null;
          if (bootstrap) { clearTimeout(live.openTimer); received(bootstrap); return; }
          inspectFrame(live, frame, first);
          if (first) received(null);
          first = false; clearTimeout(live.openTimer);
          if (state.live !== live) return;
          watchStatus("Live changes connected."); void drainWatch(live);
        }
        if (buffer.length > 2 * 1024 * 1024) throw new RPCError("The live frame exceeds the read limit.", "response_too_large");
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  }
  async function drainWatch(live) {
    if (state.live !== live || state.work || live.busy) return;
    live.busy = true;
    try {
      while (state.live === live && live.dirty) {
        live.dirty = false;
        const signal = AbortSignal.any([live.controller.signal, AbortSignal.timeout(15000)]);
        const snapshot = bounded(await rpc("view.run", homeParams(), signal));
        if (state.live !== live) return;
        renderHome(snapshot, true); watchStatus("Live changes connected · stored rows refreshed.");
      }
    } catch (error) { liveFailure(live, error); }
    finally { live.busy = false; }
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
  // Presence returns normalized output. Raw input normalization and typed
  // invalid_params belong to session.open; reject malformed read responses.
  function presenceDisplayName(entry) {
    if (!Object.hasOwn(entry, "displayName")) return "";
    const label = entry.displayName;
    if (typeof label !== "string" || !label || Array.from(label).length > 64 || /[\p{Cc}\p{Cf}\p{Cs}]/u.test(label) || /^\p{White_Space}|\p{White_Space}$/u.test(label)) {
      throw new RPCError("The presence display label is unreadable.", "invalid_response");
    }
    return label;
  }
  async function presence(signal) {
    if (!state.methods.has("presence.list")) { message($("presence-rows"), "unavailable", "Presence unavailable.", "This server does not expose live presence."); return; }
    const result = await rpc("presence.list", { project: state.config.project }, signal);
    if (!Array.isArray(result.presence)) throw new RPCError("The presence response is unreadable.", "invalid_response");
    clear($("presence-rows"));
    if (!result.presence.length) { message($("presence-rows"), "empty", "No active sessions.", "Presence is ephemeral; no session is visible in this project."); return; }
    result.presence.slice(0, 5).forEach((entry) => {
      const row = document.createElement("div"); row.className = "presence-row";
      const principalRef = entry && entry.principal;
      if (!principalRef || typeof principalRef.kind !== "string" || !principalRef.kind || typeof principalRef.id !== "string" || !principalRef.id) throw new RPCError("The presence principal is unreadable.", "invalid_response");
      const kind = principalRef.kind, id = principalRef.id;
      const displayName = presenceDisplayName(entry);
      const delegated = typeof entry.onBehalfOf === "string" && entry.onBehalfOf ? entry.onBehalfOf : "";
      const principal = delegated ? "on behalf of " + delegated : (id && id !== "anonymous" ? kind + ":" + id : kind);
      const initial = document.createElement("span"); initial.className = "presence-initial"; text(initial, principal.slice(0, 1).toUpperCase());
      const content = document.createElement("div");
      const name = document.createElement("div"); name.className = "presence-name"; text(name, principal);
      const identity = document.createElement("div"); identity.className = "presence-identity";
      const anonymousUnavailable = "Anonymous principal · client label unavailable";
      text(identity, kind === "anonymous" && !delegated ? (displayName ? "Anonymous principal" : anonymousUnavailable) : kind + ":" + id + " principal");
      content.append(name, identity);
      if (displayName) {
        const label = document.createElement("div"); label.className = "presence-display-name";
        text(label, "Client label (untrusted): " + displayName); content.appendChild(label);
      }
      const working = Array.isArray(entry.workingOn) ? entry.workingOn.filter((resource) => typeof resource === "string" && resource) : [];
      const shown = working.slice(0, 3);
      const workingLabel = shown.length ? "Working on " + shown.join(", ") : "No claimed work shared";
      const remainder = working.length - shown.length;
      const focus = document.createElement("div"); focus.className = "presence-focus"; text(focus, [entry.focus ? "Focus " + entry.focus : "No focus shared", workingLabel + (remainder > 0 ? " + " + remainder + " more" : "")].join(" · ")); content.appendChild(focus);
      const scope = document.createElement("span"); scope.className = "presence-scope"; text(scope, working.length + " working");
      row.append(initial, content, scope); $("presence-rows").appendChild(row);
    });
    if (result.presence.length > 5) {
      const remainder = document.createElement("p"); remainder.className = "elision"; text(remainder, (result.presence.length - 5) + " more visible sessions are hidden to keep this view compact."); $("presence-rows").appendChild(remainder);
    }
  }
  function directBlockers(value) {
    const invalid = () => { throw new RPCError("The direct blocker response is unreadable.", "invalid_response"); };
    if (!value || !Array.isArray(value.edges) || value.edges.length > 100 || !Number.isInteger(value.hiddenCount) || value.hiddenCount < 0 || value.hiddenCount > 100) invalid();
    return { hiddenCount: value.hiddenCount, edges: value.edges.map((edge) => {
      if (!edge || !["blocks", "conditional-blocks"].includes(edge.kind) || !["satisfied", "unsatisfied", "unknown"].includes(edge.state)) invalid();
      if (Object.hasOwn(edge, "reason") && (typeof edge.reason !== "string" || Array.from(edge.reason).length > 128)) invalid();
      const ref = edge.blocker;
      if (!ref || typeof ref !== "object" || Array.isArray(ref)) invalid();
      const id = Object.hasOwn(ref, "id"), key = Object.hasOwn(ref, "key");
      if (id === key || Object.keys(ref).some((field) => !["id", "key", "branch"].includes(field))) invalid();
      if (id && (typeof ref.id !== "string" || (Object.hasOwn(ref, "branch") && !integer(ref.branch)))) invalid();
      if (key && (typeof ref.key !== "string" || Object.hasOwn(ref, "branch"))) invalid();
      const branch = Object.hasOwn(ref, "branch") ? String(ref.branch) : "0";
      // The existing form strips CR/LF and trims its ID; Explain also refuses
      // Unicode edge whitespace. Never turn a canonical ref into another ID.
      const selectable = id && !!ref.id && branch === "0" && ref.id.trim() === ref.id && !/^\p{White_Space}|\p{White_Space}$|[\r\n]/u.test(ref.id);
      return { ...edge, label: id ? ref.id + (branch !== "0" ? " · branch " + branch : "") : "Key " + ref.key, selectable };
    }) };
  }
  function renderDirectBlockers(node, result) {
    const heading = document.createElement("h3"); text(heading, "Direct blocker verdicts"); node.appendChild(heading);
    if (!result.edges.length) {
      const empty = document.createElement("p"); text(empty, result.hiddenCount ? "Only unreadable direct blockers were returned." : "No direct blocker edges in this snapshot."); node.appendChild(empty);
    }
    const list = document.createElement("ul"); list.className = "explain-direct-blockers";
    result.edges.forEach((edge) => {
      const row = document.createElement("li"); row.dataset.state = edge.state;
      const ref = document.createElement(edge.selectable ? "a" : "span"); ref.className = "blocker-ref"; text(ref, edge.label);
      if (edge.selectable) {
        const url = new URL(location.href); url.searchParams.set("view", "explain"); url.searchParams.set("itemId", edge.blocker.id); url.hash = ""; ref.href = url.href;
        ref.addEventListener("click", (event) => {
          if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
          event.preventDefault(); $("explain-id").value = edge.blocker.id; load(false);
        });
      }
      const verdict = document.createElement("span"); verdict.className = "blocker-verdict";
      text(verdict, ({ satisfied: "Satisfied", unsatisfied: "Unsatisfied", unknown: "Unknown" })[edge.state] + " · " + edge.kind);
      const reason = document.createElement("span"); reason.className = "blocker-reason"; text(reason, edge.reason || "No direct reason supplied.");
      row.append(ref, verdict, reason); list.appendChild(row);
    });
    node.appendChild(list);
    const boundary = document.createElement("p"); boundary.className = "muted"; text(boundary, "Verdicts describe the returned snapshot. Opening an item reads its current explanation."); node.appendChild(boundary);
  }
  async function explainMembership(signal, itemId) {
    const node = $("explain-state");
    if (!state.methods.has("view.explain") || !state.config.blockedViewId) { message(node, "unavailable", "Explanation unavailable.", "A server-backed saved view and its explanation method are not configured."); return; }
    if (!itemId) { text(node, "Enter an item ID to request its explanation."); return; }
    const result = await rpc("view.explain", { viewId: state.config.blockedViewId, itemId }, signal);
    const direct = Object.hasOwn(result, "directBlockers") ? directBlockers(result.directBlockers) : null;
    clear(node);
    const summary = document.createElement("p"); text(summary, result.included ? "Included in the view." : "Outside the current view."); node.appendChild(summary);
    const reasons = document.createElement("ul"); reasons.className = "explain-reasons";
    const suppliedReasons = Array.isArray(result.reasons) ? result.reasons.filter((reason) => typeof reason === "string" && reason.trim() && reason !== "included") : [];
    suppliedReasons.forEach((reason) => { const li = document.createElement("li"); text(li, reason); reasons.appendChild(li); });
    if (suppliedReasons.length) node.appendChild(reasons);
    else if (!direct) {
      const unavailable = document.createElement("p"); unavailable.className = "explain-unavailable";
      text(unavailable, result.included ? "No direct blocker reason was supplied by the server for this item." : "No direct exclusion reason was supplied by the server for this item."); node.appendChild(unavailable);
    }
    if (!direct) { const unavailable = document.createElement("p"); unavailable.className = "direct-blockers-unavailable"; text(unavailable, "Direct blocker verdicts unavailable on this server."); node.appendChild(unavailable); }
    if (direct) renderDirectBlockers(node, direct);
    const hidden = direct ? direct.hiddenCount : count(result.hiddenBlockers);
    if (hidden) { const copy = document.createElement("p"); copy.className = "explain-hidden-blockers"; text(copy, hidden + " blockers cannot be read with current permissions."); node.appendChild(copy); }
    if (result.readinessPending === true) { const pending = document.createElement("p"); pending.className = "explain-pending"; text(pending, "Readiness is still being evaluated by the server."); node.appendChild(pending); }
  }
  function commentValue(value) {
    const bad = () => { throw new RPCError("The comment provenance is unreadable.", "invalid_response"); };
    if (!value || typeof value !== "object" || Array.isArray(value)) bad();
    const timestamp = input => typeof input === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(input) && Number.isFinite(Date.parse(input));
    const imported = Object.hasOwn(value, "origin");
    const allowed = imported ? ["body", "principal", "time", "origin", "sourceTime", "sourceAuthor"] : ["body", "principal", "time"];
    if (Object.keys(value).some(key => !allowed.includes(key)) || typeof value.body !== "string" || !value.principal || typeof value.principal.kind !== "string" || !value.principal.kind || typeof value.principal.id !== "string" || !value.principal.id || !timestamp(value.time)) bad();
    if (imported) {
      if (value.origin !== "import" || !timestamp(value.sourceTime)) bad();
      if (Object.hasOwn(value, "sourceAuthor") && (typeof value.sourceAuthor !== "string" || !value.sourceAuthor || value.sourceAuthor.replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "") !== value.sourceAuthor || [...value.sourceAuthor].length > 256 || /[\p{Cc}\p{Cf}\p{Cs}]/u.test(value.sourceAuthor))) bad();
    }
    return value;
  }
  async function explain(signal) {
    const node = $("comment-state"), id = $("explain-id").value.trim();
    await explainMembership(signal, id);
    if (signal.aborted) return;
    if (!id) { text(node, "Select an item to read comments."); return; }
    if (!state.methods.has("bead.get")) { text(node, "Comments unavailable: item reads are not configured."); return; }
    const result = await rpc("bead.get", { id: { id, branch: 0 } }, signal);
    if (signal.aborted) return;
    if (!result || !result.bead || result.bead.id !== id) throw new RPCError("The item read does not match this selection.", "invalid_response");
    if (!Object.hasOwn(result, "comments")) { text(node, "No comments supplied for this item."); return; }
    if (!Array.isArray(result.comments)) throw new RPCError("The comments are unreadable.", "invalid_response");
    const comments = result.comments.map(commentValue);
    if (!comments.length) { text(node, "No comments for this item."); return; }
    clear(node);
    comments.forEach(comment => {
      const row = document.createElement("article"); row.className = "comment-row";
      const actor = document.createElement("p"); actor.className = "comment-actor"; text(actor, "Verified actor: " + comment.principal.kind + ":" + comment.principal.id);
      const at = document.createElement("p"); text(at, (comment.origin === "import" ? "Restored at: " : "Comment time: ") + comment.time);
      const body = document.createElement("p"); body.className = "comment-body"; text(body, comment.body); row.append(actor, at, body);
      if (comment.origin === "import") {
        const provenance = document.createElement("p"); provenance.className = "comment-source";
        text(provenance, "Origin: import · Source author (untrusted): " + (comment.sourceAuthor || "not supplied") + " · Source time (untrusted): " + comment.sourceTime); row.appendChild(provenance);
      }
      node.appendChild(row);
    });
  }
  async function epic(signal) {
    const id = $("epic-id").value;
    if (!id) { text($("epic-state"), "Enter an epic ID to read its progress."); return; }
    requireMethod("view.run");
    const summary = await PlugEpicProgress.read({ id, rpc, signal, config: state.config, asOfParams });
    signal.throwIfAborted();
    PlugEpicProgress.show(summary, state.config.maxRows);
    text($("epic-state"), "Counts include only children you can see.");
    text($("snapshot-state"), "Epic snapshot rev " + summary.asOf.revision);
    text($("epic-progress-heading"), "Epic progress");
    text($("epic-progress-state"), "Open Epic progress to read a bounded direct-child summary.");
  }
  function target(view) { return $({ home: "changed-rows", next: "next-rows", blocked: "blocked-state", presence: "presence-rows", explain: "explain-state", epic: "epic-state" }[view]); }
  async function load(nextPage) {
    if (state.work) state.work.controller.abort();
    stopWatch();
    PlugEpicProgress.clear();
    text($("comment-state"), "Comments await a completed item read.");
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
      else if (view === "epic") await epic(work.controller.signal);
      if (state.work !== work) return;
      setConnection("ok", "Connected"); setMessage("");
    } catch (error) {
      if (state.work !== work) return;
      if (authenticationFailure(error)) return;
      if (view === "epic") {
        PlugEpicProgress.clear();
        // Parent refusal details are not presentation data. Do not copy a
        // server hint that could contain an unreadable identity or status.
        const known = ["invalid_response", "invalid_params", "method_not_found", "unknown_field", "scope_too_large", "response_exceeds_budget", "cursor_invalid", "cursor_expired", "incarnation_changed", "history_gone", "forbidden", "not_found", "unavailable"];
        error = new RPCError("Epic progress is unavailable. Refresh to request a new snapshot.", known.includes(error && error.code) ? error.code : "unavailable");
      }
      stopWatch();
      text($("comment-state"), "Comments unavailable: this read did not complete.");
      if (work.liveFailure) return;
      if (view === "home" && state.methods.has("view.run")) watchStatus(work.controller.signal.aborted ? "Live changes cancelled. Refresh to reconnect." : "Live changes refused · refresh required.");
      if (work.controller.signal.aborted) {
        const detail = work.timedOut ? "The server read timed out. Refresh to try again." : "Request cancelled; nothing changed.";
        setConnection("loading", work.timedOut ? "Timed out" : "Cancelled"); setMessage(detail, "cancelled"); message(target(view), "cancelled", work.timedOut ? "Timed out." : "Read cancelled.", detail);
      } else { setConnection("error", "Unavailable"); setMessage(failure(error), "error"); message(target(view), "error", "This read was refused.", failure(error)); }
    } finally {
      clearTimeout(timer);
      if (state.work === work) { state.work = null; $("cancel").hidden = true; $("refresh").disabled = false; if (state.live) void drainWatch(state.live); }
    }
  }
  function forgetIdentity(abort = true) {
    ++credentialGeneration;
    if (abort && state.work) { state.work.controller.abort(); state.work = null; }
    stopWatch();
    state.bridges.forEach((revoke) => revoke()); state.bridges = [];
    accessToken = ""; authenticatedSeen.clear(); $("auth-token").value = "";
    state.hello = null; state.methods = new Set();
    state.home = null; state.next = null; state.blocked = null;
    PlugEpicProgress.clear();
    ["changed-rows", "next-rows", "blocked-state", "explain-state", "comment-state", "presence-rows", "plugin-frames", "catalog-entries"].forEach((id) => clear($(id)));
    ["changed-more", "next-more", "blocked-more"].forEach((id) => { clear($(id)); $(id).hidden = true; });
    ["changed-count", "ready-count", "revision-count", "presence-count"].forEach((id) => text($(id), "—"));
    ["changed-caption", "ready-caption", "revision-caption", "presence-caption", "snapshot-state"].forEach((id) => text($(id), "Sign-in required"));
    text($("epic-progress-heading"), "Epic progress unavailable.");
    text($("epic-progress-state"), "Sign in to read epic progress.");
    text($("footer-auth"), "Sign-in required");
    $("authentication").hidden = false; $("auth-form").hidden = false; $("sign-out").hidden = true;
    text($("auth-state"), "Sign in with a current key for this server. The key stays in this tab until sign-out or reload.");
    if (abort) { $("cancel").hidden = true; $("refresh").disabled = false; }
  }
  $("auth-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const credential = $("auth-token").value;
    forgetIdentity();
    if (!credential || credential.length > 4096 || /\s/.test(credential)) {
      text($("auth-state"), "Enter a valid key for this server."); return;
    }
    accessToken = credential;
    load(false);
  });
  $("sign-out").addEventListener("click", () => {
    forgetIdentity(); text($("epic-state"), "Sign in to read epic progress.");
    setConnection("loading", "Signed out"); setMessage("Signed out.");
  });
  function selectView(name) {
    state.activeView = name;
    document.querySelectorAll(".tab").forEach((tab) => { const active = tab.dataset.view === name; tab.classList.toggle("is-active", active); tab.setAttribute("aria-selected", String(active)); });
    document.querySelectorAll(".view-panel").forEach((panel) => { panel.hidden = panel.dataset.panel !== name; });
    load(false);
  }
  $("refresh").addEventListener("click", () => load(false));
  $("cancel").addEventListener("click", () => { if (state.work) state.work.controller.abort(); });
  $("stop-watch").addEventListener("click", () => { stopWatch(); watchStatus("Live changes cancelled. Refresh to reconnect."); });
  window.addEventListener("pagehide", () => { forgetIdentity(); watchStatus("Live changes stopped. Refresh to reconnect."); });
  $("explain-form").addEventListener("submit", (event) => { event.preventDefault(); load(false); });
  $("epic-form").addEventListener("submit", (event) => { event.preventDefault(); load(false); });
  document.querySelectorAll(".tab").forEach((tab) => tab.addEventListener("click", () => selectView(tab.dataset.view)));
  const requested = new URLSearchParams(location.search).get("view");
  const requestedItem = new URLSearchParams(location.search).get("itemId");
  if (requested === "explain" && requestedItem) $("explain-id").value = requestedItem;
  const requestedEpic = new URLSearchParams(location.search).get("epicId");
  if (requested === "epic" && requestedEpic) $("epic-id").value = requestedEpic;
  selectView(["next", "blocked", "explain", "presence", "epic"].includes(requested) ? requested : "home");
}());
