# Prepared host caller contract

This packet prepares six script changes; it installs nothing. All callers take
`--binding /absolute/path/host-binding.json`. The binding has one `endpoint`, one
`incarnation`, an expected transport `principal`, an informational `agent`, and
exactly two project entries named `cody-ops` and `signals-v2`. Each entry has an
absolute `workspace` and an explicit lossless `legacy_ids` object mapping original
IDs to canonical `{id: ...}` refs. Project names and workspace selection are never
inferred from cwd, BEADS_DIR, PLUG_SERVER, or labels. A token may be supplied only
via the binding's named `token_env`; it is never saved in journal/diagnostics.

Commands/RPC: `server.hello`, `agent.brief`, `bead.get`, `bead.create`,
`bead.update`, `bead.link`, `bead.unlink`, `bead.close`, `bead.reopen`, `tx.apply`,
`view.run`. Mutation responses use Commit.results[i].ref, never stdout parsing.
All project requests carry `_meta.project` and `_meta.agent` (server.hello has no metadata member); writes have durable random
idempotency keys. Principal comes from transport and must match agent.brief.whoami.

Legacy original IDs remain byte-for-byte Python strings in sidecars and JSONL;
the per-project mapping determines the canonical ref. New IDs use the returned
canonical ref and require a create receipt scoped to this instance/project.
Unmapped sidecar IDs are refused; no direct-ID identity fallback is guessed. IDs are never split, case-folded, shortened, or prefix-matched.
Dependency direction: dependent -> prerequisite, kind `blocks`. Parent is
child -> parent. Graph targets must resolve in the same bound project.

The field vocabulary is the imported ontology vocabulary: type is a separate
create member; title/description/priority/labels/design/acceptance_criteria,
assignee/due_at/external_ref are fields. Open/closed map through the explicitly
prepared host ontology slots, not arbitrary default Plug type names. An ontology
prepared with this packet must be installed by the separate restore/host lane;
these callers never apply ontology or restore a store.

Sidecar protocol: one workspace flock; intent journal fsynced before each RPC;
exact key/request replay after an unknown result; frozen initial sidecar image;
recovery only if each local file still equals its initial or staged target image.
Multi-file replacement is recoverable, not atomically visible to unrelated readers.
A server transaction and local files are not claimed to share a transaction.

Molecules use an ordinary typed ChangeSet template: permanent root plus five
ordered children implement/verify/self-review/review/land; each successor blocks
on its predecessor; root closes only after every child; selected tasks block on
root. This supplies the old root/steps/id_mapping shape deliberately. It does not
claim x.workflow.invoke output is a Beads molecule. Runtime close guards are
mandatory. Source template parity against private store contents remains a
switch-day verification requirement.

Topology preparation: one authoritative Plug server/store with two projects,
clients use that endpoint. No Dolt remote or refs/dolt/data push/pull. No sync.run
is issued: no supported destination is bound yet. Backup, replica/sync destination
and live credentials/incarnation/import maps are explicit deployment inputs, not
invented by this packet. No deployed cloud factory exists per binding931-937.

Review fixes: every projection request checks the bound identity and current
project visibility before the 15-second disk reuse decision. Only a typed
transport outage can return marked stale data; an authority/result refusal
invalidates the cache. An ordinary close is a canonical CloseOp plus typed
metadata i8.host.close_reason in one tx.apply revision, preserving the current
reason without inferring lifecycle meaning from arbitrary comments. Reopen
projections omit the old reason. Other close-producing adapters should use the
same metadata convention; arbitrary external closes have no typed core reason
slot in the pinned read envelope. Completed and recovered pours return the
persisted id_mapping, including the stdout-loss boundary.

Closed plan imports use a topological close order satisfying dependency
prerequisites and child-before-parent constraints. Open/missing prerequisites or
children and closure cycles are refused before creating anything. Core close
guards remain enabled. Scoped create receipts are refreshed when another
prepared caller has written the workspace journal.
