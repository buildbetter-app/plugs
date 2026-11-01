(function () {
  "use strict";

  // Presentation only. The view.run producer owns parent selection, current
  // authorization, status interpretation, snapshot binding and read caps.
  const put = (node, value) => { node.textContent = String(value); };
  function clear() {
    document.getElementById("epic-counts").replaceChildren();
    document.getElementById("epic-remainder").replaceChildren();
    document.getElementById("epic-snapshot").replaceChildren();
  }
  function show(summary, maxGroups) {
    clear();
    const invalid = () => { const error = new Error("The server returned an invalid progress summary."); error.code = "invalid_response"; throw error; };
    const natural = (value) => Number.isSafeInteger(value) && value >= 0;
    if (!summary || !Array.isArray(summary.counts) || !Number.isInteger(maxGroups) || maxGroups < 1 || maxGroups > 100 || summary.counts.length > maxGroups || !natural(summary.visibleCount) || !natural(summary.hiddenCount)) invalid();
    let visible = 0;
    for (const group of summary.counts) {
      if (!group || typeof group.label !== "string" || !group.label || !natural(group.count)) invalid();
      visible += group.count;
      if (!Number.isSafeInteger(visible)) invalid();
    }
    if (visible !== summary.visibleCount || !summary.asOf || !/^(0|[1-9][0-9]*)$/.test(String(summary.asOf.revision)) || (typeof summary.asOf.revision === "number" && !natural(summary.asOf.revision)) || typeof summary.asOf.incarnation !== "string" || !summary.asOf.incarnation) invalid();
    const node = document.getElementById("epic-counts");
    const total = document.createElement("p"); total.className = "epic-visible-total";
    put(total, summary.visibleCount + " visible children"); node.appendChild(total);
    if (summary.counts.length) {
      const list = document.createElement("dl"); list.className = "epic-status-counts";
      summary.counts.forEach(({ label, count }) => {
        const row = document.createElement("div");
        const status = document.createElement("dt"); put(status, label);
        const number = document.createElement("dd"); put(number, count);
        row.append(status, number); list.appendChild(row);
      });
      node.appendChild(list);
    } else {
      const empty = document.createElement("p"); empty.className = "muted";
      put(empty, "No visible direct children in this snapshot."); node.appendChild(empty);
    }
    // Never combine this remainder with a status count or completion ratio.
    if (summary.hiddenCount) put(document.getElementById("epic-remainder"), summary.hiddenCount + " more you cannot see");
    put(document.getElementById("epic-snapshot"), "Snapshot rev " + summary.asOf.revision);
  }
  globalThis.PlugEpicProgress = Object.freeze({ clear, show });
})();
