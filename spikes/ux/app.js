(() => {
  "use strict";

  // This adapter deliberately generates a stable 20,000-bead corpus in the
  // browser. It is a fixture for the view contract, not a connection to bd
  // serve. The UI only renders the selected budget, never the full corpus.
  const people = [
    { id: "agent-01", name: "Nikhil Shinday", initials: "NS", color: "", focus: "reviewing the launch plan" },
    { id: "agent-02", name: "Maya Chen", initials: "MC", color: "avatar-green", focus: "API contract cleanup" },
    { id: "agent-03", name: "Owen Wright", initials: "OW", color: "avatar-purple", focus: "queue arbitration" },
    { id: "agent-04", name: "Priya Shah", initials: "PS", color: "avatar-orange", focus: "migration guardrails" },
    { id: "agent-05", name: "Leo Martin", initials: "LM", color: "avatar-green", focus: "UI catalog examples" },
  ];
  const agents = people.concat(Array.from({ length: 45 }, (_, index) => ({
    id: `agent-${String(index + 6).padStart(2, "0")}`,
    name: `Fixture agent ${String(index + 6).padStart(2, "0")}`,
    initials: `F${String(index + 6).padStart(2, "0")}`,
    color: index % 3 === 0 ? "avatar-purple" : index % 3 === 1 ? "avatar-orange" : "avatar-green",
    focus: "fixture workload",
  })));
  const epics = [
    "Shared server readiness", "Agent handoff", "Query budget", "History and restore",
    "Authorization chain", "Plugin catalog", "Dolt standby", "CLI parity",
  ];
  const titleParts = [
    "Add cursor expiry notice", "Document lease recovery", "Tighten ready ordering",
    "Expose revision in list row", "Verify project stamp", "Trim duplicate watcher events",
    "Pin migration checkpoint", "Add blocked reason", "Review plugin grant", "Refresh API examples",
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
      // Spread ownership across all 50 fixture agents even when a sorted view
      // selects every 960th fixture row.
      const owner = agents[(i + Math.floor(i / 23)) % agents.length];
      const statusRoll = random();
      const candidateStatus = statusRoll < 0.39 ? "in_progress" : statusRoll < 0.86 ? "open" : "closed";
      // Spread repeated fixture cycles by their cycle number so a recent
      // timeline shows a readable cadence instead of twenty identical "0m"
      // labels when the 20k corpus wraps its 16-hour window.
      const changedMinutes = ((i * 37) % 960) + Math.floor(i / 960);
      // Dependencies always point at earlier rows, so their status is already
      // known. A non-closed candidate with an unresolved dependency becomes
      // blocked; blockedBy and reason are therefore derived from relations.
      const dependencyIndexes = [];
      if (i >= 1 && i % 3 === 0) dependencyIndexes.push(i - 1);
      if (i >= 7 && i % 11 === 0) dependencyIndexes.push(i - 7);
      if (i >= 31 && i % 37 === 0) dependencyIndexes.push(i - 31);
      const dependencies = dependencyIndexes.map((index) => result[index]);
      const unresolvedDependencies = dependencies.filter((dependency) => dependency.status !== "closed");
      const status = unresolvedDependencies.length ? "blocked" : candidateStatus;
      const epic = pick(epics, i * 11 + 2);
      const dependencyIds = dependencies.map((dependency) => dependency.id);
      const unresolvedDependencyIds = unresolvedDependencies.map((dependency) => dependency.id);
      const relations = dependencyIds.map((targetId) => ({ type: "depends_on", targetId }));
      relations.push({ type: "belongs_to_epic", targetId: epic });
      const blockedBy = unresolvedDependencyIds.join(", ");
      const reason = blockedBy
        ? `waiting on ${blockedBy}`
        : dependencyIds.length
          ? `ready after ${dependencyIds.join(", ")}`
          : "ready with no dependencies";
      result.push({
        id: `pl-${String(i + 1).padStart(5, "0")}`,
        title: `${pick(titleParts, i * 13 + Math.floor(i / 17) + 3)} · ${epic}`,
        status,
        priority: (i * 3) % 5,
        owner,
        epic,
        relations,
        dependencyIds,
        unresolvedDependencyIds,
        blockedBy,
        reason,
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
  const corpusById = new Map(corpus.map((item) => [item.id, item]));

  function esc(value) {
    return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[char]));
  }
  function statusSymbol(status) { return `<span class="status-symbol status-${status === "in_progress" ? "progress" : status}">${statusIcons[status]}</span>`; }
  function age(minutes) { return minutes < 60 ? `${minutes}m in fixture` : `${Math.floor(minutes / 60)}h in fixture`; }
  function visible(items) { return items.slice(0, state.budget); }
  function elision(items) {
    const hidden = Math.max(0, items.length - state.budget);
    return `<tr class="elision-row"><td colspan="5"><strong>${state.budget} shown</strong> · ${hidden.toLocaleString()} more stay behind the view budget · sort order is stable</td></tr>`;
  }
  function beadTitle(bead) {
    return `<div class="bead-title">${esc(bead.title)}</div><div class="bead-id">${bead.id} · P${bead.priority}</div>`;
  }
  function beadTable(items, heading, subheading, action) {
    const rows = visible(items).map((bead) => `<tr class="bead-row" data-bead-id="${bead.id}" data-status="${bead.status}" data-dependency-ids="${esc(bead.dependencyIds.join(","))}" data-unresolved-dependency-ids="${esc(bead.unresolvedDependencyIds.join(","))}">
      <td class="bead-state">${statusSymbol(bead.status)}</td>
      <td>${beadTitle(bead)}</td>
      <td class="bead-meta">${bead.status === "blocked" ? `<span class="pill pill-blocked">${esc(bead.reason)}</span>` : `<span class="pill pill-${bead.status === "in_progress" ? "progress" : bead.status}">${statusLabels[bead.status]}</span>`}</td>
      <td class="bead-owner">${esc(bead.owner.name.split(" ")[0])}</td>
      <td class="bead-time">${age(bead.changedMinutes)}</td>
    </tr>`).join("");
    return `<section class="panel"><div class="panel-heading"><div><h2>${heading}</h2><p>${subheading}</p></div>${action ? `<button type="button" class="panel-action" data-view="${action.view}">${action.label}</button>` : ""}</div>
      <table class="bead-table"><colgroup><col style="width:35px"><col><col style="width:190px"><col style="width:108px"><col style="width:91px"></colgroup><thead><tr><th></th><th>Bead</th><th>Signal</th><th>Owner</th><th>Changed</th></tr></thead><tbody>${rows || `<tr><td colspan="5"><div class="empty-state">Nothing to show in this bounded view.</div></td></tr>`}${items.length > state.budget ? elision(items) : ""}</tbody></table></section>`;
  }

  function isReady(item) {
    return item.status === "open" && dependencyRelations(item).every((relation) => corpusById.get(relation.targetId)?.status === "closed");
  }

  function dependencyRelations(item) { return item.relations.filter((relation) => relation.type === "depends_on"); }
  function unresolvedRelations(item) {
    return dependencyRelations(item).filter((relation) => corpusById.get(relation.targetId)?.status !== "closed");
  }

  function readyItems() { return corpus.filter(isReady); }
  function blockedItems() { return corpus.filter((item) => item.status === "blocked"); }
  function changedItems() { return corpus.filter((item) => item.changedMinutes < 90); }
  function activeAgentCount() {
    return new Set(corpus.filter((item) => item.status !== "closed").map((item) => item.owner.id)).size;
  }
  function activeCountFor(person) {
    return corpus.filter((item) => item.owner.id === person.id && item.status !== "closed").length;
  }
  function blockedCountFor(person) {
    return corpus.filter((item) => item.owner.id === person.id && item.status === "blocked").length;
  }
  function viewMetrics(view) {
    const candidates = view === "next" ? readyItems().length
      : view === "blocked" ? blockedItems().length
      : view === "changes" ? corpus.length
        : view === "home" ? changedItems().length
          : view === "people" ? people.length
            : view === "epic" ? epics.length
              : view === "catalog" ? Object.keys(catalogData).length : 0;
    const budgeted = ["home", "next", "blocked", "changes"].includes(view);
    return { candidates, visible: budgeted ? Math.min(candidates, state.budget) : candidates };
  }

  function summaryCards() {
    const ready = readyItems().length;
    const blocked = blockedItems();
    const changed = changedItems().length;
    const activeAgents = activeAgentCount();
    const humanDecisionCount = blocked.filter((item) => unresolvedRelations(item).some((relation) => corpusById.get(relation.targetId)?.status === "in_progress")).length;
    return `<div class="signal-grid">
      <div class="signal-card"><div class="signal-label">Ready next</div><div class="signal-value">${ready.toLocaleString()}</div><div class="signal-detail">ordered by dependency and priority</div></div>
      <div class="signal-card"><div class="signal-label">Blocked</div><div class="signal-value">${blocked.length.toLocaleString()}</div><div class="signal-detail attention">${humanDecisionCount.toLocaleString()} blocked by active work</div></div>
      <div class="signal-card"><div class="signal-label">Changed in fixture</div><div class="signal-value">${changed.toLocaleString()}</div><div class="signal-detail">within the deterministic fixture window</div></div>
      <div class="signal-card"><div class="signal-label">Agents active</div><div class="signal-value">${activeAgents}</div><div class="signal-detail">of ${agents.length} fixture agents</div></div>
    </div>`;
  }

  function homeView() {
    const changed = changedItems().sort((a, b) => a.changedMinutes - b.changedMinutes);
    return `<div class="page-heading"><div><div class="eyebrow">Fixture home view</div><h1>Changes since I looked</h1><p>A calm starting point: recent fixture signals first, with the rest available on demand.</p></div><div class="last-read">fixture snapshot</div></div>${summaryCards()}${beadTable(changed, "Recent signals", "Only fixture changes that can alter your next move are promoted here.", { view: "changes", label: "See all changes" })}`;
  }

  function nextView() {
    const ready = readyItems().sort((a, b) => a.priority - b.priority || a.dependencyIds.length - b.dependencyIds.length || a.changedMinutes - b.changedMinutes);
    const rows = visible(ready).map((bead) => `<div class="queue-row" data-bead-id="${bead.id}" data-status="${bead.status}" data-dependency-ids="${esc(bead.dependencyIds.join(","))}" data-unresolved-dependency-ids="${esc(bead.unresolvedDependencyIds.join(","))}"><div>${statusSymbol(bead.status)}</div><div><div class="queue-title">${esc(bead.title)}</div><div class="queue-reason">${esc(bead.reason)} · P${bead.priority}</div></div><div class="queue-owner">${esc(bead.owner.name.split(" ")[0])}</div><button type="button" class="queue-action">Claim next</button></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Fixture queue view</div><h1>What next</h1><p>Dependency-aware fixture work, capped at ${state.budget} visible nodes.</p></div><div class="last-read">${ready.length.toLocaleString()} ready candidates</div></div><section class="panel"><div class="panel-heading"><div><h2>Suggested next moves</h2><p>Every row explains its dependency state and priority. Claiming one does not reorder the fixture.</p></div><button type="button" class="panel-action" id="sort-next">Why this order?</button></div><div class="queue-list">${rows}</div>${ready.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${ready.length - state.budget} ready candidates hidden by budget</div>` : ""}</section>`;
  }

  function blockedView() {
    const blocked = blockedItems().sort((a, b) => a.changedMinutes - b.changedMinutes || a.priority - b.priority);
    const visibleBlocked = visible(blocked);
    const visibleBlockedIds = new Set(visibleBlocked.map((item) => item.id));
    const outsideCauseCount = visibleBlocked.filter((item) => unresolvedRelations(item).some((relation) => !visibleBlockedIds.has(relation.targetId))).length;
    const rows = visible(blocked).map((bead) => `<div class="blocked-row" data-bead-id="${bead.id}" data-status="${bead.status}" data-dependency-ids="${esc(bead.dependencyIds.join(","))}" data-unresolved-dependency-ids="${esc(bead.unresolvedDependencyIds.join(","))}"><div>${statusSymbol("blocked")}</div><div><div class="queue-title">${esc(bead.title)}</div><div class="bead-id">${bead.id} · blocked by ${esc(bead.blockedBy)}</div></div><div class="blocked-owner">${esc(bead.reason)}</div></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Fixture attention view</div><h1>Why blocked</h1><p>Reasons are attached to each row; hidden fixture work never inflates the count.</p></div><div class="last-read"><span class="pill pill-blocked">${blocked.length.toLocaleString()} blocked</span></div></div><section class="panel"><div class="panel-heading"><div><h2>Waiting on a decision or dependency</h2><p>Blockers are sorted by the most recent fixture change, then by priority.</p></div><button type="button" class="panel-action">Export list</button></div><div class="blocked-list">${rows}</div>${blocked.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${blocked.length - state.budget} blocked beads hidden by budget</div>` : ""}</section><section class="panel"><div class="panel-heading"><div><h2>Blocked without a visible cause</h2><p>One relation-derived count keeps the fixture useful without drawing a noisy edge map.</p></div></div><div class="empty-state">${outsideCauseCount.toLocaleString()} visible blocked beads depend on a cause outside this bounded list.</div></section>`;
  }

  function changesView() {
    const changed = corpus.slice().sort((a, b) => a.changedMinutes - b.changedMinutes);
    const rows = visible(changed).map((bead) => `<div class="timeline-row" data-bead-id="${bead.id}" data-status="${bead.status}" data-dependency-ids="${esc(bead.dependencyIds.join(","))}" data-unresolved-dependency-ids="${esc(bead.unresolvedDependencyIds.join(","))}"><div class="timeline-time">${age(bead.changedMinutes)}</div><div>${statusSymbol(bead.status)}</div><div class="timeline-copy"><strong>${esc(bead.owner.name.split(" ")[0])}</strong> <span>${esc(bead.changeKind)} on ${esc(bead.title)}</span></div><div class="timeline-actor">${esc(bead.id)}</div></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Fixture delta sample</div><h1>What changed</h1><p>Fixture events are sorted by age so movement does not create visual churn.</p></div><div class="last-read">fixture snapshot · no live watch</div></div><section class="panel"><div class="panel-heading"><div><h2>Fixture change window</h2><p>These updates are generated locally for this snapshot; no server watch is connected.</p></div><button type="button" class="panel-action">Replay fixture</button></div><div class="timeline-list">${rows}</div>${changed.length > state.budget ? `<div class="elision-row" style="padding:12px;text-align:center"><strong>${state.budget} shown</strong> · ${changed.length - state.budget} older changes hidden by budget</div>` : ""}</section>`;
  }

  function peopleView() {
    const rows = people.map((person) => {
      const active = activeCountFor(person);
      const blocked = blockedCountFor(person);
      return `<div class="person-detail-row"><div class="person-avatar ${person.color}">${person.initials}</div><div class="person-name">${esc(person.name)}<div class="person-focus">${esc(person.focus)} · fixture focus</div></div><div class="bead-meta">${active.toLocaleString()} active beads · ${blocked.toLocaleString()} blocked</div><div class="person-load">${active.toLocaleString()} active</div></div>`;
    }).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Fixture presence</div><h1>Who is on what</h1><p>Five visible fixture collaborators answer the presence question without claiming a live feed.</p></div><div class="last-read">${people.length} fixture people visible</div></div><section class="panel"><div class="panel-heading"><div><h2>Active collaborators</h2><p>Counts are derived from the fixture's owner and status relations.</p></div><button type="button" class="panel-action">Open presence settings</button></div><div class="people-table">${rows}</div></section>`;
  }

  function epicView() {
    const rows = epics.map((epic) => {
      const beads = corpus.filter((item) => item.relations.some((relation) => relation.type === "belongs_to_epic" && relation.targetId === epic));
      const done = beads.filter((item) => item.status === "closed").length;
      const blocked = beads.filter((item) => item.status === "blocked").length;
      const ready = beads.filter(isReady).length;
      const complete = beads.length ? Math.round(done * 100 / beads.length) : 0;
      return `<div class="epic-row"><div class="epic-title">${epic}</div><div class="epic-count">${done.toLocaleString()}/${beads.length.toLocaleString()}</div><div class="progress-track"><div class="progress-fill ${complete > 78 ? "pass" : complete < 52 ? "warn" : ""}" style="width:${complete}%"></div></div><div class="epic-percent">${complete}% complete · ${blocked} blocked · ${ready} ready</div></div>`;
    }).join("");
    return `<div class="page-heading"><div><div class="eyebrow">Fixture roll-up view</div><h1>Is the epic on track?</h1><p>One line per epic, with counts derived from bead status and epic membership.</p></div><div class="last-read">fixture revision · deterministic</div></div><section class="panel"><div class="panel-heading"><div><h2>Epic health</h2><p>Progress is a bounded summary over the same 20,000-bead fixture.</p></div><button type="button" class="panel-action">Define a view</button></div><div class="epic-list">${rows}</div></section>`;
  }

  const catalogData = {
    panel: { title: "Queue summary", body: "A compact panel represented by fixture catalog data.", code: '{ "kind": "panel",\n  "id": "queue-summary",\n  "view": "ready-next",\n  "budget": 25\n}' },
    column: { title: "Blocked by", body: "A declarative column that explains a relationship.", code: '{ "kind": "column",\n  "field": "blockedBy",\n  "format": "reference"\n}' },
    button: { title: "Claim next", body: "A button maps to a fixture method example; no custom drawing needed.", code: '{ "kind": "button",\n  "label": "Claim next",\n  "method": "bead.claimNext"\n}' },
  };
  function catalogView() {
    const entries = Object.entries(catalogData).map(([key, item]) => `<div class="catalog-spec"><h3>${item.title}</h3><p>${item.body}</p><div class="catalog-code">${esc(item.code)}</div><button type="button" class="queue-action" style="margin-top:10px" data-catalog="${key}">Preview ${key}</button></div>`).join("");
    return `<div class="page-heading"><div><div class="eyebrow">D31 extension surface</div><h1>Declarative catalog</h1><p>Simple panels, columns, and buttons stay data-first; iframes are reserved for custom logic and drawing.</p></div><div class="last-read">3 entries · 0 iframes</div></div><section class="panel catalog-page"><div class="panel-heading"><div><h2>Catalog entries</h2><p>These examples can sit beside a sandboxed plugin without turning the home view into a plugin canvas.</p></div><span class="catalog-badge">declarative</span></div><div class="catalog-grid">${entries}</div></section>`;
  }

  function renderMain() {
    const views = { home: homeView, next: nextView, blocked: blockedView, changes: changesView, people: peopleView, epic: epicView, catalog: catalogView };
    mainView.innerHTML = (views[state.view] || homeView)();
    const metrics = viewMetrics(state.view);
    mainView.dataset.view = state.view;
    mainView.dataset.budget = state.budget;
    mainView.dataset.candidateCount = metrics.candidates;
    mainView.dataset.visibleCount = metrics.visible;
    document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => { state.view = button.dataset.view; update(); }));
    document.querySelectorAll("[data-catalog]").forEach((button) => button.addEventListener("click", () => { state.catalog = button.dataset.catalog; renderCatalogPreview(); if (button.closest(".catalog-page")) { state.view = "catalog"; update(); } }));
  }
  function renderCatalogPreview() {
    const item = catalogData[state.catalog];
    document.getElementById("catalog-preview").textContent = `${item.title}\n${item.code}`;
    document.querySelectorAll(".catalog-row").forEach((row) => row.classList.toggle("is-selected", row.dataset.catalog === state.catalog));
  }
  function renderPeople() {
    document.getElementById("people-list").innerHTML = people.map((person) => `<div class="person-row"><div class="person-avatar ${person.color}">${person.initials}</div><div><div class="person-name">${esc(person.name)}</div><div class="person-focus">${esc(person.focus)} · fixture focus</div></div><div class="person-count">${activeCountFor(person)}</div></div>`).join("");
  }
  function update() {
    document.querySelectorAll(".view-tab").forEach((button) => button.classList.toggle("is-active", button.dataset.view === state.view));
    document.querySelectorAll("[data-budget]").forEach((button) => button.classList.toggle("is-active", Number(button.dataset.budget) === state.budget));
    document.getElementById("snapshot-count").textContent = state.budget;
    document.getElementById("blocked-count").textContent = blockedItems().length.toLocaleString();
    renderMain();
    renderCatalogPreview();
  }
  document.querySelectorAll("[data-budget]").forEach((button) => button.addEventListener("click", () => { state.budget = Number(button.dataset.budget); update(); }));
  document.querySelectorAll(".catalog-row").forEach((button) => button.addEventListener("click", () => { state.catalog = button.dataset.catalog; renderCatalogPreview(); }));
  document.getElementById("reset-look").addEventListener("click", () => { document.querySelector(".last-read").textContent = "fixture snapshot · just now"; });
  renderPeople();
  update();
})();
