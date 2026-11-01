# Parent-aware epic progress

Done = an authenticated `view.run` returns exact visible direct-child counts
and only the hidden remainder from canonical persisted parents under one
content/ontology snapshot and current authority, proven by the bound HTTP
tests and exact-head focused/whole-tree RunsOn evidence; a mocked scope or
successful schema validation alone does not count.

The trusted host calls `NewEpicProgressConfig(checker, ontologySource)` and
sets `composition.Config.EpicProgress` (or `viewops.Config.EpicProgress`) before
binding views. Nil leaves the capability unavailable. The adapter delegates
policy to EP2's checker. It does not enable hierarchy writes, project moves,
or the UI, and it does not add a method or wire member.

An existing anonymous host with no authentication or authorization plugin may
explicitly call `NewAnonymousEpicProgressConfig(realStore, ontologySource)`.
This separate constructor retains the real pinned content, durable ontology,
incarnation and caps, and returns complete visible counts without an
authorization elision. It accepts only the anonymous principal. Composition
refuses it with authenticated mode, a provider/session store, or authorization
installed; viewops also refuses combining it with a read authorizer. A missing
fixed checker never selects this mode: `NewEpicProgressConfig(nil, source)`
continues to fail. Anonymous browser evidence and authenticated fixed-policy
evidence are separate claims.

```json
{
  "definition": {
    "filter": { "query": "parent=epic-id" },
    "fields": [],
    "groupBy": ["epic-progress"]
  },
  "_meta": { "project": "parent-project", "agent": "client-label" }
}
```

The query is exactly one canonical bare branch-zero parent ID. The project
selects the authorized parent scope. Direct children remain in their own
projects and retain their own read permissions; unrelated beads and
grandchildren are excluded. Additional filters, sort, or payload fields refuse.
`budgetTokens` is absent or zero. Nonzero refuses `invalid_params`: tokens are
not rows, and this count is complete rather than truncated or approximated.

The existing result has `items: []`, `asOf`, and groups with empty `items`:

- `total.count` is the number of visible direct children.
- `status:<canonical status ID>` counts only visible children. Status IDs are
  validated against each child's durable ontology in the same content reader;
  the response does not infer labels, readiness, or completion policy.
- Only `total.elided` may contain `{count: N, reason: "authorization",
  continueWith: {method: "view.run", params: ...}}`. Clients render the literal
  “N more you can’t see”. The params preserve the definition/view ID and
  project/agent, while dropping `cursor` and `asOf`. This is a fresh refresh
  and reauthorization, never an expansion or pagination of hidden rows.

The ordinary 20,000 **authorized** candidate cap applies; the type-summary
100,000 exception does not. MaxGroups=100 includes `total`. An incomplete,
over-limit, unavailable-ontology or changed-authority result refuses without
partial counts. There is no `nextCursor`; any supplied cursor is invalid.
Historical `asOf` uses its pinned content and ontology with current authority.
Restore/history errors are preserved, and a refresh obtains new snapshots.

An unavailable host returns `invalid_params` with
`details.capability="epic-progress"` and `details.available=false`. A hidden or
missing parent returns `not_found` without counts. Existing authentication and
typed authorization refusals remain in force. The serialization interpretation
was approved by controller v4 on 2026-11-01; runtime, independent review, EP5/EP6
qualification, G2/G3, release and production remain separate evidence gates.
