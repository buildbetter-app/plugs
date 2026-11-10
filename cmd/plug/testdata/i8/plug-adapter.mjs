/**
 * Heartbeads 0.6.6 Plug projection adapter.
 *
 * This module is copied into the Heartbeads package by the unapplied patch
 * packet.  It deliberately has no Beads/JSONL fallback: an explicit binding
 * selects one Plug endpoint and the two named projects.
 */

import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isAbsolute } from "node:path";

const PROJECT_NAMES = ["cody-ops", "signals-v2"];
const DEFAULT_STALE_AFTER_MS = 30_000;
// Literal categories admitted by the core category contract. Native names are
// never converted; these sets only select lifecycle/display behavior.
const CATEGORIES = ["active", "wip", "done", "frozen", "triage", "backlog", "unstarted", "started", "completed", "canceled"];
const TERMINAL_CATEGORIES = new Set(["done", "completed", "canceled"]);
const COMPLETED_CATEGORIES = new Set(["done", "completed"]);

export class BindingError extends Error {
  constructor(message) {
    super(message);
    this.name = "BindingError";
  }
}

/** A transport interruption may use the last complete publication as stale data. */
export class TransportError extends Error {
  constructor(message, cause) {
    super(message, { cause });
    this.name = "TransportError";
  }
}

/** A binding, envelope, or authority/result refusal must never be hidden by stale data. */
export class AuthorityError extends Error {
  constructor(message) {
    super(message);
    this.name = "AuthorityError";
  }
}

function object(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BindingError(`${label} must be an object`);
  }
  return value;
}

function string(value, label) {
  if (typeof value !== "string" || value.length === 0) {
    throw new BindingError(`${label} must be a non-empty string`);
  }
  return value;
}

function canonicalPrincipal(value, label) {
  const principal = object(value, label);
  string(principal.kind, `${label}.kind`);
  string(principal.id, `${label}.id`);
  if (principal.sponsor !== undefined || principal.caveats !== undefined) {
    throw new BindingError(`${label} must not carry sponsor or caveats`);
  }
  return { kind: principal.kind, id: principal.id };
}

function exactProjectMap(value) {
  const projects = object(value, "binding.projects");
  const keys = Object.keys(projects).sort();
  if (keys.length !== PROJECT_NAMES.length || keys.some((key, i) => key !== PROJECT_NAMES[i])) {
    throw new BindingError("binding.projects must contain exactly cody-ops and signals-v2");
  }
  const normalized = {};
  for (const name of PROJECT_NAMES) {
    const project = object(projects[name], `binding.projects.${name}`);
    const workspace = string(project.workspace, `binding.projects.${name}.workspace`);
    if (!isAbsolute(workspace)) {
      throw new BindingError(`binding.projects.${name}.workspace must be absolute`);
    }
    const legacyIds = object(project.legacy_ids, `binding.projects.${name}.legacy_ids`);
    const normalizedIds = Object.create(null);
    for (const [legacy, ref] of Object.entries(legacyIds)) {
      if (typeof legacy !== "string" || legacy.length === 0) {
        throw new BindingError(`binding.projects.${name}.legacy_ids contains an empty ID`);
      }
      const canonical = object(ref, `binding.projects.${name}.legacy_ids.${legacy}`);
      const id = string(canonical.id, `binding.projects.${name}.legacy_ids.${legacy}.id`);
      if (Object.keys(canonical).some((key) => key !== "id")) {
        throw new BindingError(`binding.projects.${name}.legacy_ids.${legacy} has unknown fields`);
      }
      normalizedIds[legacy] = { id };
    }
    normalized[name] = { workspace, legacy_ids: normalizedIds };
  }
  const canonicalIds = PROJECT_NAMES.flatMap((name) => Object.values(normalized[name].legacy_ids).map((ref) => ref.id));
  if (new Set(canonicalIds).size !== canonicalIds.length) {
    throw new BindingError("binding legacy_ids must not reuse a canonical id");
  }
  return normalized;
}

export function validateBinding(raw) {
  const binding = object(raw, "binding");
  if (binding.schema !== "plug-host-binding/1") {
    throw new BindingError("binding.schema must be plug-host-binding/1");
  }
  const endpoint = string(binding.endpoint, "binding.endpoint");
  let parsed;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new BindingError("binding.endpoint must be an absolute URL");
  }
  if (!/^https?:$/.test(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) {
    throw new BindingError("binding.endpoint must be an HTTP(S) URL without credentials/query/fragment");
  }
  if (parsed.protocol === "http:" && !["127.0.0.1", "localhost", "::1"].includes(parsed.hostname)) {
    throw new BindingError("non-loopback binding.endpoint must use HTTPS");
  }
  if (parsed.pathname !== "" && parsed.pathname !== "/" && parsed.pathname !== "/rpc/v1") {
    throw new BindingError("binding.endpoint must be an origin or exact /rpc/v1 endpoint");
  }
  const rpcEndpoint = new URL(parsed.toString());
  rpcEndpoint.pathname = "/rpc/v1";
  rpcEndpoint.search = "";
  rpcEndpoint.hash = "";
  const incarnation = string(binding.incarnation, "binding.incarnation");
  const principal = canonicalPrincipal(binding.principal, "binding.principal");
  const agent = string(binding.agent, "binding.agent");
  if (binding.token_env !== undefined) string(binding.token_env, "binding.token_env");
  return Object.freeze({
    schema: binding.schema,
    endpoint,
    rpcEndpoint: rpcEndpoint.toString(),
    incarnation,
    principal,
    agent,
    token_env: binding.token_env,
    projects: Object.freeze(exactProjectMap(binding.projects)),
  });
}

export async function loadBinding(path) {
  if (typeof path !== "string" || !isAbsolute(path)) {
    throw new BindingError("--binding must be an absolute path");
  }
  let raw;
  try {
    raw = JSON.parse(await readFile(path, "utf8"));
  } catch {
    throw new BindingError(`unable to read binding ${path}`);
  }
  return validateBinding(raw);
}

function envToken(binding) {
  if (!binding.token_env) return undefined;
  return process.env[binding.token_env];
}

function samePrincipal(a, b) {
  return a && b && a.kind === b.kind && a.id === b.id;
}

function refID(value) {
  if (typeof value === "string") return value;
  if (value && typeof value === "object" && typeof value.id === "string") return value.id;
  throw new Error("Plug response contained a reference without an id");
}

function clone(value) {
  return structuredClone(value);
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function sha256(value) {
  return createHash("sha256").update(stableJson(value), "utf8").digest("hex");
}

function graphKey(project, canonical) {
  return `${project}\u0000${canonical}`;
}

function lifecycleCloseReason(bead, category) {
  if (!COMPLETED_CATEGORIES.has(category)) return undefined;
  const value = bead?.metadata?.["i8.host.close_reason"];
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  if (value.schema !== "plug-host-close-reason/1" || typeof value.reason !== "string") return undefined;
  return value.reason;
}

export class PlugClient {
  constructor(binding, fetchImpl = globalThis.fetch) {
    this.binding = validateBinding(binding);
    if (typeof fetchImpl !== "function") throw new Error("fetch is required");
    this.fetch = fetchImpl;
    this.sequence = 0;
  }

  async rpc(project, method, params = {}) {
    if (!PROJECT_NAMES.includes(project)) throw new BindingError(`unknown project ${project}`);
    if (params && Object.prototype.hasOwnProperty.call(params, "_meta")) {
      throw new BindingError("callers cannot override Plug transport metadata");
    }
    const requestParams = method === "server.hello"
      ? { ...params }
      : { ...params, _meta: { project, agent: this.binding.agent } };
    if (method === "server.hello" && Object.prototype.hasOwnProperty.call(requestParams, "_meta")) {
      throw new BindingError("server.hello must not carry _meta");
    }
    const body = {
      jsonrpc: "2.0",
      id: `heartbeads-${++this.sequence}`,
      method,
      params: requestParams,
    };
    const headers = { "content-type": "application/json", accept: "application/json", "Plug-RPC": "1" };
    const token = envToken(this.binding);
    if (token) headers.authorization = `Bearer ${token}`;
    let response;
    try {
      response = await this.fetch(this.binding.rpcEndpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
    } catch (error) {
      throw new TransportError(`Plug ${method} transport failed`, error);
    }
    if (!response.ok) {
      if (response.status >= 500 || response.status === 408 || response.status === 429) {
        throw new TransportError(`Plug ${method} returned HTTP ${response.status}`);
      }
      throw new AuthorityError(`Plug ${method} returned HTTP ${response.status}`);
    }
    let envelope;
    try {
      envelope = await response.json();
    } catch (error) {
      // A 2xx body that cannot be parsed is a result/envelope refusal. It is
      // not a transport interruption and therefore must invalidate the cache.
      throw new AuthorityError(`Plug ${method} returned invalid JSON`);
    }
    if (!envelope || envelope.jsonrpc !== "2.0" || envelope.id !== body.id) {
      throw new AuthorityError(`Plug ${method} returned an invalid JSON-RPC envelope`);
    }
    if (envelope.error) {
      const code = envelope.error?.data?.code || envelope.error?.code || "remote_error";
      throw new AuthorityError(`Plug ${method} failed: ${code}`);
    }
    if (!("result" in envelope)) throw new AuthorityError(`Plug ${method} omitted result`);
    return envelope.result;
  }

  async verify() {
    // Recheck the live instance and transport principal on every publication;
    // a cache entry is never reused across an incarnation or visibility change.
    const hello = await this.rpc(PROJECT_NAMES[0], "server.hello", {});
    if (hello?.incarnation !== this.binding.incarnation) {
      throw new AuthorityError("Plug incarnation does not match the binding");
    }
    const who = await this.rpc(PROJECT_NAMES[0], "agent.brief", {});
    if (!samePrincipal(who?.whoami?.principal, this.binding.principal)) {
      throw new AuthorityError("transport principal mismatch from agent.brief");
    }
  }

  async page(project, definition = {
    filter: {},
    fields: ["fields", "metadata", "project", "status", "labels", "parent"],
    sort: ["id"],
    budgetTokens: 100000,
  }) {
    const items = [];
    let cursor;
    let asOf;
    let pages = 0;
    do {
      const params = { definition };
      if (cursor) {
        params.cursor = cursor;
        params.asOf = asOf;
      }
      const result = await this.rpc(project, "view.run", params);
      if (!Array.isArray(result?.items)) throw new AuthorityError(`Plug view.run returned no items for ${project}`);
      if (!result.asOf || typeof result.asOf !== "object") {
        throw new AuthorityError(`Plug view.run omitted its pinned snapshot for ${project}`);
      }
      items.push(...result.items);
      cursor = result.nextCursor || "";
      if (!asOf) asOf = result.asOf;
      else if (stableJson(asOf) !== stableJson(result.asOf)) {
        throw new AuthorityError(`${project} view pagination changed its pinned snapshot`);
      }
      pages++;
      if (pages > 10_000) throw new AuthorityError(`Plug view.run pagination exceeded the safety bound for ${project}`);
    } while (cursor);
    return { items, pages, asOf };
  }

  async receiptIndex(project) {
    const entries = Object.entries(this.binding.projects[project].legacy_ids);
    const byCanonical = new Map(entries.map(([legacy, ref]) => [ref.id, legacy]));
    const workspace = this.binding.projects[project].workspace;
    let raw;
    try {
      raw = JSON.parse(await readFile(`${workspace}/.plug-host-journal.json`, "utf8"));
    } catch (error) {
      if (error?.code === "ENOENT") return { entries, byCanonical, receipts: [] };
      throw new AuthorityError(`${project} create receipt journal is unreadable`);
    }
    if (!raw || raw.schema !== "plug-host-journal/1" || !raw.jobs || typeof raw.jobs !== "object") {
      throw new AuthorityError(`${project} create receipt journal has an unknown schema`);
    }
    // The retained producer hashes the exact original binding string.
    // Keep origin slashes/default ports/host spelling instead of URL-normalizing
    // the receipt scope; normalization is only for the HTTP request endpoint.
    const scopes = new Set([sha256({ endpoint: this.binding.endpoint, incarnation: this.binding.incarnation, principal: this.binding.principal, project })]);
    const receipts = [];
    for (const job of Object.values(raw.jobs)) {
      if (!job || !scopes.has(job.scope) || !Array.isArray(job.calls)) continue;
      for (const call of job.calls) {
        if (!call || !call.result || typeof call.result !== "object") continue;
        const attempts = Array.isArray(call.attempts) ? call.attempts : [];
        const params = attempts.length ? attempts[attempts.length - 1]?.params : undefined;
        const ops = call.method === "tx.apply" ? params?.changeSet?.ops : null;
        const refs = call.method === "bead.create"
          ? [call.result?.results?.[0]?.ref]
          : Array.isArray(ops) ? ops.flatMap((op, index) => op?.kind === "create" ? [call.result?.results?.[index]?.ref] : []) : [];
        for (const ref of refs) {
          if (!ref || typeof ref.id !== "string" || ref.id.length === 0 || Object.keys(ref).some((key) => key !== "id")) {
            throw new AuthorityError(`${project} create receipt has an invalid canonical ref`);
          }
          if (!byCanonical.has(ref.id)) byCanonical.set(ref.id, ref.id);
          receipts.push(ref.id);
        }
      }
    }
    return { entries, byCanonical, receipts };
  }

  async installedOntology(project) {
    const result = await this.rpc(project, "ontology.get", {});
    if (result?.drifted !== false || (result.problems != null && (!Array.isArray(result.problems) || result.problems.length))) {
      throw new AuthorityError(`${project} installed ontology is drifted or malformed`);
    }
    const document = object(result.document, `${project} ontology.document`);
    const statuses = object(document.statuses, `${project} ontology.statuses`);
    const categories = Object.create(null);
    for (const [name, definition] of Object.entries(statuses)) {
      const category = definition?.category;
      if (!CATEGORIES.includes(category)) {
        throw new AuthorityError(`${project} installed status ${name} has unsupported category ${category}`);
      }
      categories[name] = category;
    }
    if (!Object.keys(categories).length) throw new AuthorityError(`${project} installed ontology has no statuses`);
    return { categories, revision: result.revision, fingerprint: sha256({ document, revision: result.revision }) };
  }

  async readyAt(project, asOf) {
    const ids = new Set();
    let cursor;
    let pages = 0;
    do {
      const result = await this.rpc(project, "bead.ready", { limit: 1000, ...(cursor ? { cursor } : {}) });
      if (!Array.isArray(result?.items) || stableJson(result.asOf) !== stableJson(asOf)) {
        throw new AuthorityError(`${project} readiness does not match the pinned bead snapshot`);
      }
      for (const item of result.items) ids.add(string(item?.id, `${project} ready bead.id`));
      cursor = result.nextCursor || "";
      if (++pages > 10000) throw new AuthorityError(`${project} readiness pagination exceeded the safety bound`);
    } while (cursor);
    return ids;
  }

  async projectSnapshot(project) {
    const ontology = await this.installedOntology(project);
    const { items, pages, asOf } = await this.page(project);
    const { byCanonical } = await this.receiptIndex(project);
    const nodes = [];
    const nodeByCanonical = new Map();
    const detailByCanonical = new Map();
    for (const item of items) {
      const canonical = string(item?.id, `${project} bead.id`);
      const legacy = byCanonical.get(canonical);
      if (!legacy) throw new AuthorityError(`${project} bead ${canonical} has no lossless legacy_ids mapping`);
      const detail = await this.rpc(project, "bead.get", { id: { id: canonical }, asOf });
      const bead = object(detail?.bead, `${project} bead ${canonical}`);
      if (bead.id !== canonical || bead.project !== project) {
        throw new AuthorityError(`${project} bead ${canonical} resolved outside the bound project`);
      }
      detailByCanonical.set(canonical, bead);
      const fields = object(bead.fields || item.fields || {}, `${project} bead ${canonical}.fields`);
      const status = string(bead.status || item.status || fields.status, `${project} bead ${canonical}.status`);
      const statusCategory = ontology.categories[status];
      if (!statusCategory) throw new AuthorityError(`${project} bead ${canonical} has unknown installed ontology status ${status}`);
      if (!Number.isInteger(fields.priority) || fields.priority < 0 || fields.priority > 4) {
        throw new AuthorityError(`${project} bead ${canonical} priority must be an integer from 0 through 4`);
      }
      const labels = Array.isArray(bead.labels) ? bead.labels.slice() : (Array.isArray(item.labels) ? item.labels.slice() : (Array.isArray(fields.labels) ? fields.labels.slice() : []));
      const node = {
        id: legacy,
        legacyId: legacy,
        canonicalId: canonical,
        graphKey: graphKey(project, canonical),
        project,
        title: typeof fields.title === "string" ? fields.title : "",
        description: typeof fields.description === "string" ? fields.description : undefined,
        status,
        statusCategory,
        priority: fields.priority,
        issueType: typeof bead.type === "string" ? bead.type : (typeof item.type === "string" ? item.type : (typeof fields.type === "string" ? fields.type : "issue")),
        owner: typeof fields.owner === "string" ? fields.owner : undefined,
        assignee: typeof bead.assignee === "string" ? bead.assignee : (typeof item.assignee === "string" ? item.assignee : (typeof fields.assignee === "string" ? fields.assignee : undefined)),
        createdBy: typeof fields.created_by === "string" ? fields.created_by : undefined,
        createdAt: bead.createdAt || item.createdAt || fields.created_at || "",
        updatedAt: bead.updatedAt || item.updatedAt || fields.updated_at || "",
        closedAt: TERMINAL_CATEGORIES.has(statusCategory) ? fields.closed_at : undefined,
        closeReason: lifecycleCloseReason(bead, statusCategory),
        acceptanceCriteria: fields.acceptance_criteria,
        estimatedMinutes: fields.estimated_minutes,
        // The binding keeps original IDs opaque.  Project is the stable
        // namespace used for colors and filtering; never split an ID.
        prefix: project,
        labels,
        blockerCount: 0,
        dependentCount: 0,
        blockerIds: [],
        dependentIds: [],
        blockerKeys: [],
        dependentKeys: [],
        childIds: [],
        childKeys: [],
        parentId: undefined,
        parentKey: undefined,
      };
      nodes.push(node);
      nodeByCanonical.set(canonical, node);
    }

    const dependencies = [];
    const links = [];
    const seen = new Set();
    const addLink = (fromCanonical, toCanonical, kind, createdAt) => {
      const from = nodeByCanonical.get(fromCanonical);
      const to = nodeByCanonical.get(toCanonical);
      if (!from || !to) throw new AuthorityError(`${project} graph link references an unreadable bead`);
      const fromLegacy = from.id;
      const toLegacy = to.id;
      const fromKey = from.graphKey;
      const toKey = to.graphKey;
      const key = `${fromKey}->${toKey}:${kind}`;
      if (seen.has(key)) return;
      seen.add(key);
      if (kind === "blocks") {
        // Plug host binding is dependent -> prerequisite. Heartbeads draws
        // the prerequisite first, so the public projection reverses it.
        links.push({ source: toLegacy, target: fromLegacy, sourceKey: toKey, targetKey: fromKey, type: kind, createdAt });
        dependencies.push({ issue_id: fromLegacy, depends_on_id: toLegacy, issue_key: fromKey, depends_on_key: toKey, type: kind, created_at: createdAt || "" });
        from.dependentIds.push(toLegacy);
        from.dependentKeys.push(toKey);
        to.blockerIds.push(fromLegacy);
        to.blockerKeys.push(fromKey);
      } else if (kind === "parent-child") {
        links.push({ source: toLegacy, target: fromLegacy, sourceKey: toKey, targetKey: fromKey, type: kind, createdAt });
        dependencies.push({ issue_id: fromLegacy, depends_on_id: toLegacy, issue_key: fromKey, depends_on_key: toKey, type: kind, created_at: createdAt || "" });
        from.parentId = toLegacy;
        from.parentKey = toKey;
        to.childIds.push(fromLegacy);
        to.childKeys.push(fromKey);
      } else {
        links.push({ source: fromLegacy, target: toLegacy, sourceKey: fromKey, targetKey: toKey, type: "relates_to", createdAt });
        dependencies.push({ issue_id: toLegacy, depends_on_id: fromLegacy, issue_key: toKey, depends_on_key: fromKey, type: "relates_to", created_at: createdAt || "" });
      }
    };
    for (const item of items) {
      const source = string(item.id, `${project} graph source`);
      const bead = detailByCanonical.get(source);
      const createdAt = bead?.updatedAt || item.updatedAt || "";
      for (const relation of Array.isArray(bead?.links) ? bead.links : []) {
        const kind = string(relation.kind, `${project} graph edge kind`);
        addLink(source, refID(relation.to), kind, relation.createdAt || createdAt);
      }
      if (bead?.parent && typeof bead.parent === "object") {
        throw new AuthorityError(`${project} graph parent for ${source} is hidden or malformed`);
      }
      if (typeof bead?.parent === "string" && bead.parent.length > 0) {
        addLink(source, bead.parent, "parent-child", createdAt);
      }
    }
    for (const node of nodes) {
      node.blockerCount = node.blockerIds.length;
      node.dependentCount = node.dependentIds.length;
    }
    const readyCanonical = await this.readyAt(project, asOf);
    for (const id of readyCanonical) if (!nodeByCanonical.has(id)) throw new AuthorityError(`${project} ready result references an unreadable bead`);
    for (const node of nodes) node.ready = readyCanonical.has(node.canonicalId);
    const open = new Set(nodes.filter((node) => !TERMINAL_CATEGORIES.has(node.statusCategory)).map((node) => node.graphKey));
    const blocked = new Set(dependencies.filter((edge) => edge.type === "blocks" && open.has(edge.depends_on_key)).map((edge) => edge.issue_key));
    const ready = nodes.filter((node) => node.ready).map((node) => node.id);
    const stats = {
      total: nodes.length,
      open: nodes.filter((node) => ["active", "unstarted"].includes(node.statusCategory)).length,
      inProgress: nodes.filter((node) => ["wip", "started"].includes(node.statusCategory)).length,
      blocked: blocked.size,
      closed: nodes.filter((node) => TERMINAL_CATEGORIES.has(node.statusCategory)).length,
      frozen: nodes.filter((node) => node.statusCategory === "frozen").length,
      byStatus: Object.fromEntries(Object.keys(ontology.categories).map((status) => [status, nodes.filter((node) => node.status === status).length])),
      byCategory: Object.fromEntries(CATEGORIES.map((category) => [category, nodes.filter((node) => node.statusCategory === category).length])),
      actionable: ready.length,
      edges: links.length,
      prefixes: [...new Set(nodes.map((node) => node.prefix))],
    };
    const issues = nodes.map((node) => ({
      id: node.id,
      legacy_id: node.legacyId || node.id,
      canonical_id: node.canonicalId,
      project: node.project,
      title: node.title,
      description: node.description,
      status: node.status,
      status_category: node.statusCategory,
      ready: node.ready,
      priority: node.priority,
      issue_type: node.issueType,
      owner: node.owner,
      assignee: node.assignee,
      created_by: node.createdBy,
      created_at: node.createdAt,
      updated_at: node.updatedAt,
      closed_at: node.closedAt,
      close_reason: node.closeReason,
      acceptance_criteria: node.acceptanceCriteria,
      estimated_minutes: node.estimatedMinutes,
      labels: node.labels,
      dependencies: dependencies.filter((edge) => edge.issue_key === node.graphKey).map(({ issue_key, depends_on_key, ...edge }) => edge),
    }));
    // ontology.get has no historical selector. Refuse a publication if the
    // project-effective document changes while its pinned bead view is read.
    const latestOntology = await this.installedOntology(project);
    if (latestOntology.fingerprint !== ontology.fingerprint) {
      throw new AuthorityError(`${project} installed ontology changed during projection`);
    }
    return { project, nodes, issues, links, dependencies, stats, ready, pages, asOf, ontology: { revision: ontology.revision, statuses: ontology.categories } };
  }
}

export class ProjectionStore {
  constructor(client, { staleAfterMs = DEFAULT_STALE_AFTER_MS } = {}) {
    this.client = client;
    this.staleAfterMs = staleAfterMs;
    this.current = null;
    this.refreshing = null;
  }

  refresh() {
    if (this.refreshing) return this.refreshing;
    const run = (async () => {
      try {
        await this.client.verify();
        const projectSnapshots = await Promise.all(PROJECT_NAMES.map((project) => this.client.projectSnapshot(project)));
        const nodes = projectSnapshots.flatMap((snapshot) => snapshot.nodes);
        const issues = projectSnapshots.flatMap((snapshot) => snapshot.issues);
        const links = projectSnapshots.flatMap((snapshot) => snapshot.links);
        const dependencies = projectSnapshots.flatMap((snapshot) => snapshot.dependencies);
        const stats = projectSnapshots.reduce((acc, snapshot) => {
          for (const key of ["total", "open", "inProgress", "blocked", "closed", "actionable", "edges"]) acc[key] += snapshot.stats[key];
          acc.prefixes.push(...snapshot.stats.prefixes);
          return acc;
        }, { total: 0, open: 0, inProgress: 0, blocked: 0, closed: 0, actionable: 0, edges: 0, prefixes: [] });
        const now = new Date().toISOString();
        const snapshot = {
          issues,
          dependencies,
          graphData: { nodes, links },
          stats: { ...stats, prefixes: [...new Set(stats.prefixes)] },
          projects: Object.fromEntries(projectSnapshots.map((project) => [project.project, {
            workspace: this.client.binding.projects[project.project].workspace,
            stats: project.stats,
            ready: project.ready,
            pages: project.pages,
            asOf: project.asOf,
            ontology: project.ontology,
          }])),
          freshness: { state: "fresh", generated_at: now, stale_after_ms: this.staleAfterMs },
        };
        this.current = snapshot;
        return clone(snapshot);
      } catch (error) {
        if (this.current && error instanceof TransportError) {
          const stale = clone(this.current);
          const ageMs = Math.max(0, Date.now() - Date.parse(stale.freshness.generated_at));
          stale.freshness = {
            ...stale.freshness,
            state: "stale",
            age_ms: ageMs,
            stale_reason: error instanceof Error ? error.message : "refresh failed",
          };
          return stale;
        }
        this.current = null;
        throw error;
      }
    })();
    this.refreshing = run;
    run.then(() => { if (this.refreshing === run) this.refreshing = null; }, () => { if (this.refreshing === run) this.refreshing = null; });
    return run;
  }

  watch(onSnapshot, onError, { intervalMs = 1000 } = {}) {
    let stopped = false;
    const tick = async () => {
      if (stopped) return;
      try {
        const snapshot = await this.refresh();
        if (!stopped) onSnapshot(snapshot);
      } catch (error) {
        if (!stopped && onError) onError(error);
      }
    };
    const timer = setInterval(tick, intervalMs);
    void tick();
    return () => { stopped = true; clearInterval(timer); };
  }
}

let singleton;
export async function projectionStore(bindingPath = process.env.HEARTBEADS_BINDING || process.env.PLUG_BINDING) {
  if (!singleton) {
    if (!bindingPath) throw new BindingError("HEARTBEADS_BINDING must name an absolute host binding");
    singleton = loadBinding(bindingPath).then((binding) => new ProjectionStore(new PlugClient(binding)));
    singleton.catch(() => { singleton = undefined; });
  }
  return singleton;
}

export async function loadProjection(bindingPath) {
  return (await projectionStore(bindingPath)).refresh();
}

export async function bindingConfig(bindingPath) {
  const binding = await loadBinding(bindingPath || process.env.HEARTBEADS_BINDING || process.env.PLUG_BINDING);
  return {
    name: "Plug",
    prefix: null,
    repoCount: PROJECT_NAMES.length,
    repos: PROJECT_NAMES,
    projects: Object.fromEntries(PROJECT_NAMES.map((name) => [name, { workspace: binding.projects[name].workspace }])),
  };
}

export async function countBindingProjects(bindingPath) {
  const config = await bindingConfig(bindingPath);
  return config.repoCount;
}
