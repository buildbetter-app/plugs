(() => {
  "use strict";

  // This adapter deliberately generates a stable 20,000-bead corpus in the
  // browser. It stands in for today's bd serve wire while the view contract is
  // explored; the UI only renders the selected budget, never the full corpus.
  const people = [
    { name: "Nikhil Shinday", initials: "NS", color: "", focus: "reviewing the launch plan", load: 3 },
    { name: "Maya Chen", initials: "MC", color: "avatar-green", focus: "API contract cleanup", load: 4 },
    { name: "Owen Wright", initials: "OW", color: "avatar-purple", focus: "queue arbitration", load: 2 },
    { name: "Priya Shah", initials: "PS", color: "avatar-orange", focus: "migration guardrails", load: 5 },
    { name: "Leo Martin", initials: "LM", color: "avatar-green", focus: "UI catalog examples", load: 3 },
  ];
  const epics = [
    "Shared server readiness", "Agent handoff", "Query budget", "History and restore",
    "Authorization chain", "Plugin catalog", "Dolt standby", "CLI parity",
  ];
  const titleParts = [
    "Add cursor expiry notice", "Document lease recovery", "Tighten ready ordering",
    "Expose revision in list row", "Verify project stamp", "Trim duplicate watcher events",
    "Pin migration checkpoint", "Add blocked reason", "Review plugin grant", "Refresh API examples",
  ];
  const blockedReasons = [
    "waiting on pl-8f2 to land", "needs a review from Maya", "guard requires an attached PR",
    "Dolt standby is still catching up", "depends on the ontology revision", "awaiting a release decision",
  ];
  const changeKinds = ["status changed", "comment added", "claim renewed", "dependency linked", "priority raised"];

  let seed = 0x51a7;
  function random() {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 4294967296;
  }
  function pick(values, index) { return values[index % values.length]; }
  function makeCorpus() {
    seed = 0x51a7;
    const result = [];
    for (let i = 0; i < 20000; i += 1) {
      // Spread ownership across the five visible collaborators even when a
      // sorted view selects every 960th fixture row.
      const owner = people[(i + Math.floor(i / 23)) % people.length];
      const statusRoll = random();
      const status = statusRoll < 0.11 ? "blocked" : statusRoll < 0.39 ? "in_progress" : statusRoll < 0.86 ? "open" : "closed";
      // Spread repeated fixture cycles by their cycle number so a recent
      // timeline shows a readable cadence instead of twenty identical "0m"
      // labels when the 20k corpus wraps its 16-hour window.
      const changedMinutes = ((i * 37) % 960) + Math.floor(i / 960);
      const blockedBy = status === "blocked" ? `pl-${String((i * 17 + 83) % 20000).padStart(5, "0")}` : "";
      result.push({
        id: `pl-${String(i + 1).padStart(5, "0")}`,
        title: `${pick(titleParts, i * 13 + Math.floor(i / 17) + 3)} · ${pick(epics, i * 7 + Math.floor(i / 17) + 2)}`,
        status,
        priority: (i * 3) % 5,
        owner,
        epic: pick(epics, i * 11 + 2),
        blockedBy,
        reason: blockedBy ? pick(blockedReasons, i * 5 + 1) : "ready when unclaimed",
        changedMinutes,
        changeKind: pick(changeKinds, i * 3 + 1),
      });
    }
    return result;
  }

  const corpus = makeCorpus();
  const query = new URLSearchParams(window.location.search);
  const allowedViews = new Set(["home", "next", "blocked", "changes", "people", "epic", "catalog"]);
  const requestedBudget = Number(query.get("budget"));
  const state = {
    budget: [25, 50, 100].includes(requestedBudget) ? requestedBudget : 50,
    view: allowedViews.has(query.get("view")) ? query.get("view") : "home",
    catalog: "panel",
  };
  const mainView = document.getElementById("main-view");
  const statusLabels = { open: "open", in_progress: "in progress", blocked: "blocked", closed: "closed" };
  const statusIcons = { open: "○", in_progress: "◐", blocked: "●", closed: "✓" };

  function esc(value) {
    return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[char]));
  }
  function statusSymbol(status) { return `<span class="status-symbol status-${status === "in_progress" ? "progress" : status}">${statusIcons[status]}</span>`; }
  function age(minutes) { return minutes < 60 ? `${minutes}m ago` : `${Math.floor(minutes / 60)}h ago`; }
  function visible(items) { return items.slice(0, state.budget); }
  function elision(items) {
    const hidden = Math.max(0, items.length - state.budget);
    return `<tr class="elision-row"><td colspan="5"><strong>${state.budget} shown</strong> · ${hidden.toLocaleString()} more stay behind the view budget · sort order is stable</td></tr>`;
  }
  function beadTitle(bead) {
    return `<div class="bead-title">${esc(bead.title)}</div><div class="bead-id">${bead.id} · P${bead.priority}</div>`;
  }
  function beadTable(items, heading, subheading, action) {
    const rows = visible(items).map((bead) => `<tr>
      <td class="bead-state">${statusSymbol(bead.status)}</td>
      <td>${beadTitle(bead)}</td>
      <td class="bead-meta">${bead.status === "blocked" ? `<span class="pill pill-blocked">${esc(bead.reason)}</span>` : `<span class="pill pill-${bead.status === "in_progress" ? "progress" : bead.status}">${statusLabels[bead.status]}</span>`}</td>
      <td class="bead-owner">${esc(bead.owner.name.split(" ")[0])}</td>
      <td class="bead-time">${age(bead.changedMinutes)}</td>
    </tr>`).join("");
    return `<section class="panel"><div class="panel-heading"><div><h2>${heading}</h2><p>${subheading}</p></div>${action ? `<button type="button" class="panel-action" data-view="${action.view}">${action.label}</button>` : ""}</div>
      <table class="bead-table"><colgroup><col style="width:35px"><col><col style="width:190px"><col style="width:108px"><col style="width:91px"></colgroup><thead><tr><th></th><th>Bead</th><th>Signal</th><th>Owner</th><th>Changed</th></tr></thead><tbody>${rows || `<tr><td colspan="5"><div class="empty-state">Nothing to show in this bounded view.</div></td></tr>`}${items.length > state.budget ? elision(items) : ""}</tbody></table></section>`;
  }

  function summaryCards() {
    const ready = corpus.filter((item) => item.status === "open").length;
    const blocked = corpus.filter((item) => item.status === "blocked").length;
    const changed = corpus.filter((item) => item.changedMinutes < 90).length;
    return `<div class="signal-grid">
      <div class="signal-card"><div class="signal-label">Ready next</div><div class="signal-value">${ready.toLocaleString()}</div><div class="signal-detail">ordered by dependency and priority</div></div>
      <div class="signal-card"><div class="signal-label">Blocked</div><div class="signal-value">${blocked.toLocaleString()}</div><div class="signal-detail attention">${Math.round(blocked / 12)} need a human decision</div></div>
      <div class="signal-card"><div class="signal-label">Changed recently</div><div class="signal-value">${changed.toLocaleString()}</div><div class="signal-detail">since this session started</div></div>
      <div class="signal-card"><div class="signal-label">People active</div><div class="signal-value">5</div><div class="signal-detail">of 50 simulated agents</div></div>
    </div>`;
  }

  function homeView() {
    const changed = corpus.filter((item) => item.changedMinutes < 90).sort((a, b) => a.changedMinutes - b.changedMinutes);
    return `<div class="page-heading"><div><div class="eyebrow">Home view</div><h1>Changes since I looked</h1><p>A calm starting point: recent signals first, with the rest available on demand.</p></div><div class="last-read">last read · 12 minutes ago</div></div>${summaryCards()}${beadTable(changed, "Recent signals", "Only changes that can alter your next move are promoted here.", { view: "changes", label: "See all changes" })}`;
  }

  function nextView() {
    const ready = corpus.filter((item) => item.status === "open").sort((a, b) => a.priority - b.priority || a.changedMinutes - b.changedMinutes);
    const rows = visible(ready).map((bead) => `<div class="queue-row"><div>${statusSymbol(bead.status)}</div><div><div class="queue-title">${esc(bead.title)}</div><div class="queue-reason">${esc(bead.reason)} · P${bead.priority}</div></div><div class="queue-owner">${esc(bead.owner.name.split(" ")[0])}</div><button type="button" class="queue-action">Claim next</button></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Queue view</div><h1>What next</h1><p>Dependency-aware work, capped at ${state.budget} visible nodes.</p></div><div class="last-read">${ready.length.toLocaleString()} ready candidates</div></div><section class="panel"><div class="panel-heading"><div><h2>Suggested next moves</h2><p>Every row explains why it is here. Claiming one does not reorder the rest.</p></div><button type="button" class="panel-action" id="sort-next">Why this order?</button></div><div class="queue-list">${rows}</div>${ready.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${ready.length - state.budget} ready candidates hidden by budget</div>` : ""}</section>`;
  }

  function blockedView() {
    const blocked = corpus.filter((item) => item.status === "blocked").sort((a, b) => a.changedMinutes - b.changedMinutes);
    const rows = visible(blocked).map((bead) => `<div class="blocked-row"><div>${statusSymbol("blocked")}</div><div><div class="queue-title">${esc(bead.title)}</div><div class="bead-id">${bead.id} · blocked by ${esc(bead.blockedBy)}</div></div><div class="blocked-owner">${esc(bead.reason)}</div></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Attention view</div><h1>Why blocked</h1><p>Reasons are attached to each row; hidden work never inflates the count.</p></div><div class="last-read"><span class="pill pill-blocked">${blocked.length.toLocaleString()} blocked</span></div></div><section class="panel"><div class="panel-heading"><div><h2>Waiting on a decision or dependency</h2><p>Blockers are sorted by the most recent change, then by priority.</p></div><button type="button" class="panel-action">Export list</button></div><div class="blocked-list">${rows}</div>${blocked.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${blocked.length - state.budget} blocked beads hidden by budget</div>` : ""}</section><section class="panel"><div class="panel-heading"><div><h2>Blocked without a visible cause</h2><p>One aggregate count keeps the view useful without drawing a noisy edge map.</p></div></div><div class="empty-state">${Math.round(blocked.length * 0.08)} blocked beads have a cause outside this view. Select the blocker to inspect it.</div></section>`;
  }

  function changesView() {
    const changed = corpus.slice().sort((a, b) => a.changedMinutes - b.changedMinutes);
    const rows = visible(changed).slice(0, 16).map((bead) => `<div class="timeline-row"><div class="timeline-time">${age(bead.changedMinutes)}</div><div>${statusSymbol(bead.status)}</div><div class="timeline-copy"><strong>${esc(bead.owner.name.split(" ")[0])}</strong> <span>${esc(bead.changeKind)} on ${esc(bead.title)}</span></div><div class="timeline-actor">${esc(bead.id)}</div></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Delta watch</div><h1>What changed</h1><p>Coalesced updates preserve row order, so movement does not create visual churn.</p></div><div class="last-read">watch · connected</div></div><section class="panel"><div class="panel-heading"><div><h2>Since 09:42 UTC</h2><p>Updates are grouped in a 250 ms window and filtered to this view.</p></div><button type="button" class="panel-action">Pause watch</button></div><div class="timeline-list">${rows}</div>${changed.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${changed.length - state.budget} older changes hidden by budget</div>` : ""}</section>`;
  }

  function peopleView() {
    const rows = people.map((person) => `<div class="person-detail-row"><div class="person-avatar ${person.color}">${person.initials}</div><div class="person-name">${esc(person.name)}<div class="person-focus">${esc(person.focus)}</div></div><div class="bead-meta">${person.load} active beads · next ${person.load < 4 ? "ready" : "review"}</div><div class="person-load">${person.load}/6 load</div></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Presence</div><h1>Who is on what</h1><p>Presence is useful when it answers a question, not when it fills a canvas.</p></div><div class="last-read">5 people visible</div></div><section class="panel"><div class="panel-heading"><div><h2>Active collaborators</h2><p>Claims show working on; focus is a short, human-readable note.</p></div><button type="button" class="panel-action">Open presence settings</button></div><div class="people-table">${rows}</div></section>`;
  }

  function epicView() {
    const rows = epics.map((epic, index) => { const complete = 38 + ((index * 13) % 54); const count = 90 + ((index * 41) % 190); return `<div class="epic-row"><div class="epic-title">${epic}</div><div class="epic-count">${Math.round(count * complete / 100)}/${count}</div><div class="progress-track"><div class="progress-fill ${complete > 78 ? "pass" : complete < 52 ? "warn" : ""}" style="width:${complete}%"></div></div><div class="epic-percent">${complete}% complete</div></div>`; }).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Roll-up view</div><h1>Is the epic on track?</h1><p>One line per epic, with counts and a trend signal instead of a graph wall.</p></div><div class="last-read">as of revision 812</div></div><section class="panel"><div class="panel-heading"><div><h2>Epic health</h2><p>Progress is a bounded summary over the same 20,000-bead corpus.</p></div><button type="button" class="panel-action">Define a view</button></div><div class="epic-list">${rows}</div></section>`;
  }

  const catalogData = {
    panel: { title: "Queue summary", body: "A compact panel backed by view.run.", code: '{ "kind": "panel",\n  "id": "queue-summary",\n  "view": "ready-next",\n  "budget": 25\n}' },
    column: { title: "Blocked by", body: "A declarative column that explains a relationship.", code: '{ "kind": "column",\n  "field": "blockedBy",\n  "format": "reference"\n}' },
    button: { title: "Claim next", body: "A button maps to a public method; no custom drawing needed.", code: '{ "kind": "button",\n  "label": "Claim next",\n  "method": "bead.claimNext"\n}' },
  };
  function catalogView() {
    const entries = Object.entries(catalogData).map(([key, item]) => `<div class="catalog-spec"><h3>${item.title}</h3><p>${item.body}</p><div class="catalog-code">${esc(item.code)}</div><button type="button" class="queue-action" style="margin-top:10px" data-catalog="${key}">Preview ${key}</button></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">D31 extension surface</div><h1>Declarative catalog</h1><p>Simple panels, columns, and buttons stay data-first; iframes are reserved for custom logic and drawing.</p></div><div class="last-read">3 entries · 0 iframes</div></div><section class="panel catalog-page"><div class="panel-heading"><div><h2>Catalog entries</h2><p>These examples can sit beside a sandboxed plugin without turning the home view into a plugin canvas.</p></div><span class="catalog-badge">declarative</span></div><div class="catalog-grid">${entries}</div></section>`;
  }

  function renderMain() {
    const views = { home: homeView, next: nextView, blocked: blockedView, changes: changesView, people: peopleView, epic: epicView, catalog: catalogView };
    mainView.innerHTML = (views[state.view] || homeView)();
    document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => { state.view = button.dataset.view; update(); }));
    document.querySelectorAll("[data-catalog]").forEach((button) => button.addEventListener("click", () => { state.catalog = button.dataset.catalog; renderCatalogPreview(); if (button.closest(".catalog-page")) { state.view = "catalog"; update(); } }));
  }
  function renderCatalogPreview() {
    const item = catalogData[state.catalog];
    document.getElementById("catalog-preview").textContent = `${item.title}\n${item.code}`;
    document.querySelectorAll(".catalog-row").forEach((row) => row.classList.toggle("is-selected", row.dataset.catalog === state.catalog));
  }
  function renderPeople() {
    document.getElementById("people-list").innerHTML = people.map((person) => `<div class="person-row"><div class="person-avatar ${person.color}">${person.initials}</div><div><div class="person-name">${esc(person.name)}</div><div class="person-focus">${esc(person.focus)}</div></div><div class="person-count">${person.load}</div></div>`).join("");
  }
  function update() {
    document.querySelectorAll(".view-tab").forEach((button) => button.classList.toggle("is-active", button.dataset.view === state.view));
    document.querySelectorAll("[data-budget]").forEach((button) => button.classList.toggle("is-active", Number(button.dataset.budget) === state.budget));
    document.getElementById("snapshot-count").textContent = state.budget;
    renderMain();
    renderCatalogPreview();
  }
  document.querySelectorAll("[data-budget]").forEach((button) => button.addEventListener("click", () => { state.budget = Number(button.dataset.budget); update(); }));
  document.querySelectorAll(".catalog-row").forEach((button) => button.addEventListener("click", () => { state.catalog = button.dataset.catalog; renderCatalogPreview(); }));
  document.getElementById("reset-look").addEventListener("click", () => { document.querySelector(".last-read").textContent = "last read · just now"; });
  renderPeople();
  update();
})();
