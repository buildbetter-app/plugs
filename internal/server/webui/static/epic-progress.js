(function () {
  "use strict";

  // Presentation only. The view.run producer owns parent selection, current
  // authorization, status interpretation, snapshot binding and read caps.
  const put = (node, value) => { node.textContent = String(value); };
  function refusal(code) {
    const error = new Error("Epic progress is unavailable for this read."); error.code = code; return error;
  }
  const object = value => value !== null && typeof value === "object" && !Array.isArray(value);
  const keys = (value, allowed) => object(value) && Object.keys(value).every(key => allowed.includes(key));
  const natural = value => Number.isSafeInteger(value) && value >= 0;
  function same(left, right) {
    if (left === right) return true;
    if (!left || !right || typeof left !== "object" || typeof right !== "object" || Array.isArray(left) !== Array.isArray(right)) return false;
    const names = Object.keys(left);
    return names.length === Object.keys(right).length && names.every(key => Object.hasOwn(right, key) && same(left[key], right[key]));
  }
  async function read({ id, rpc, signal, config, asOfParams }) {
    // A bare parent equality is the entire admitted query, never user-supplied
    // query syntax. Preserve the ID rather than trimming it into another ID.
    if (typeof id !== "string" || !id || /[\p{White_Space}\p{Cc}\p{Cf}=/]/u.test(id) || id.includes("&&")) throw refusal("invalid_params");
    const params = { definition: { filter: { query: "parent=" + id }, fields: [], groupBy: ["epic-progress"] } };
    const result = await rpc("view.run", params, signal);
    signal.throwIfAborted();
    if (!keys(result, ["items", "asOf", "groups", "projection"]) || !Array.isArray(result.items) || result.items.length || !Array.isArray(result.groups) || !keys(result.asOf, ["revision", "incarnation"])) throw refusal("invalid_response");
    asOfParams(result.asOf);
    if (result.groups.length > Math.min(100, config.maxRows + 1)) throw refusal("response_exceeds_budget");
    let total, hiddenCount = 0;
    const counts = [], seen = new Set();
    for (const group of result.groups) {
      if (!keys(group, ["key", "count", "items", "elided"]) || typeof group.key !== "string" || seen.has(group.key) || !natural(group.count) || group.count > 20000 || !Array.isArray(group.items) || group.items.length) throw refusal("invalid_response");
      seen.add(group.key);
      if (group.key === "total") {
        total = group.count;
        if (Object.hasOwn(group, "elided")) {
          const elided = group.elided;
          const refresh = elided && elided.continueWith;
          const expected = { ...params, _meta: { project: config.project } };
          if (!keys(elided, ["count", "reason", "continueWith"]) || !natural(elided.count) || elided.reason !== "authorization" || !keys(refresh, ["method", "params"]) || refresh.method !== "view.run" || !same(refresh.params, expected)) throw refusal("invalid_response");
          // This is a fresh same-scope reauthorization, not pagination. Never
          // execute a response-supplied method, scope, cursor or hidden row.
          hiddenCount = elided.count;
        }
      } else {
        if (!group.key.startsWith("status:") || group.key.length === 7 || Object.hasOwn(group, "elided")) throw refusal("invalid_response");
        counts.push({ label: group.key.slice(7), count: group.count });
      }
    }
    if (total === undefined || counts.reduce((sum, group) => sum + group.count, 0) !== total) throw refusal("invalid_response");
    return { visibleCount: total, hiddenCount, counts, asOf: result.asOf };
  }
  function clear() {
    document.getElementById("epic-counts").replaceChildren();
    document.getElementById("epic-remainder").replaceChildren();
    document.getElementById("epic-snapshot").replaceChildren();
  }
  function show(summary, maxGroups) {
    clear();
    const invalid = () => { const error = new Error("The server returned an invalid progress summary."); error.code = "invalid_response"; throw error; };
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
  globalThis.PlugEpicProgress = Object.freeze({ clear, read, show });
})();
