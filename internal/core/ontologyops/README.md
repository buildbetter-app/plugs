# Stored ontology operations

`New` borrows a read-only store and its active lease. `Config.Source` locates
the protected branch-zero `ontology` or `ontology/...` record. The package
checks the actual reference, kind and revision, calls the service-owned strict
`DecodeOntologySource`, and compiles the decoded base, overlays and plugin
availability with the pure ontology engine. Apply is only available when the
host also supplies the canonical protected Writer seam.

`Get` returns the effective project document; `Describe` can narrow it to one
type and its workflow/status definitions. `Schema` uses the real effective
schema generator. `Check` and the read results report compilation problems and
strict schema drift in actual stored bead images. These checks cover the
admitted stored document model; no separate cached blocking projection exists
in this slice to certify. Unknown outer bead members refuse explicitly.

`History` reads the real changeset log and projects its changes onto the
selected protected source, excluding unrelated row changes and internal event
descriptors. Use the last returned revision as the next integer `after`.
`Diff` reconstructs both historical source images through the driver and
returns an RFC 6902 root replacement of the effective document, or an empty
patch when equal. Current authorization applies to historical reads.

`Plan` accepts one ontology document, replacing the base when `Project` is
empty or replacing that project's overlay otherwise. Unknown document members,
including unsupported directive/gate/binding configuration, are refused by the
real decoder. The optional `typeMappings` input admits only closed `map_type`
objects with an ID, original project/type selector, and different source and
destination type keys. It uses the existing planner: the original type must be
known, the candidate destination active, and every selected row keeps its other
members. The selected mappings remain in the retained, digested plan report.
Other directive kinds and the legacy top-level `directives` input still refuse.
Useful
additions and defaults are planned against every actual stored bead row;
violating candidates return real findings rather than write anything.

The admitted migration planner receives independently enumerated inventory and
scan passes at one pinned snapshot. The host must supply its actual
`ValidatingPre` implementation. `NoValidatingPre()` is an explicit declaration
for a host with no validating-pre bindings; it must not replace a live host's
manifest. Missing bindings configuration refuses planning. The planner invokes
the real callbacks over detached affected rows. Observations of blockers and
evidence come from stored images. Missing gate or installed-authorizer
transition decisions remain explicit refusals.

The immutable report retains the candidate source, complete row images,
planner findings, explicit CAS, principal, store/incarnation, source bytes hash,
source revision and compilation digest. A fresh read checks source, row and
callback-manifest guards after planning and whenever `Planned` retrieves a
report. Source changes, row changes, changed binding manifests and changed
caller context refuse reuse. Retention is bounded and process-local; restart
or eviction requires replanning. Planner candidate revisions are consecutive
logical planning references, not a claim that a storage commit was published.

`Ready` means only that the planner found no undispositioned problem. A host
that supplies the canonical `pipeline.ProtectedApplier` may also expose
`Service.Apply`: it reloads the retained digest-bound plan, requires exact CAS
equality, rebuilds complete stored document images, and delegates one atomic
source/row/predicate transaction to the Writer. A read-only host leaves that
capability nil and does not advertise `ontology.apply`. Durable replay is
resolved by the Writer's receipt before stale source/row guards are retried.
Policy revalidation, configured-authorizer transition decisions, gate resolver
facts, other directive wire admission and multi-source planning remain unsupported.

With no authorization plugin, omit `AuthorizeRead`. With one installed, the
host must supply the current read authorizer. Planning requires access to the
whole inventory and refuses any denied row instead of silently dropping it.
Plans and source history also require read access to every configured overlay,
including projects without rows. Projectless rows refuse migration planning.
Compiler diagnostics require access to all overlays they can name. Source
history authorizes both historical source images under current permissions,
including overlays removed from the current document.

Run the focused `//internal/core/ontologyops:ontologyops_test` target through
the remote-bazel RunsOn wrapper from an isolated Plug worktree.
