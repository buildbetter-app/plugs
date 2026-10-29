# Ephemeral collaboration

The composition root creates one `Manager` with the real core service and active
storage lease, calls `collab.Bind(registry, manager)` before `rpc.New`, and closes
the manager on shutdown. It binds canonical `session.open`, `session.close`,
`presence.set`, and `presence.list`. The CLI composition is unchanged: one-shot
commands open no collaboration session. No graph write, version, changeset,
checkpoint, or dump record stores this state. A new manager starts empty.

Sessions expire on the half-open interval ending five minutes after open or the
latest successful set. Every call prunes expired sessions; an independent reaper
also releases idle memory and session contexts within one second of expiry.
List and `SessionContext` do not renew expiry. Explicit close, lifetime
cancellation, active lease loss, and shutdown cancel session contexts. Reconnect
and takeover require a new session. A session is owned by the full authenticated
principal (including sponsor/caveats); anonymous sessions additionally bind the
unverified `_meta.agent` label, which supplies no authentication or privilege.

Defaults and hard ceilings are 1,024 sessions, 64 per principal identity, 256 per
sponsor identity, 64 selected claims per session/group, 1,024 bytes per resource
or project, and 16 KiB per principal. Host configuration may lower session quotas
and the five-minute TTL. Quotas count identities independent of caveats and
client labels. Over-budget operations refuse explicitly and never return a
partial count or a truncated set of claimed resources.

Focus supports `bead/<id>` on the main branch. Set reads the actual service bead;
a project's presence never contains another project's resource. Omitted focus
preserves it; an empty string clears it. `workingOn` is a bounded selection of
resources whose current service claim belongs to the caller (including the
anonymous claim label). Omission preserves that selection; `[]` clears it.
Each list re-reads current focus and claim state as its viewer, suppressing
absent/denied/moved resources and expired or reclaimed claims without markers.

At the fixed base the service has no release or expired-claim recovery producer.
`bead.claim` of expired in-progress work explicitly refuses: time passing cannot
reopen a lifecycle slot. Tests exercise live, closed and expired claims through
the real bound service. B05 durable recovery and subsequent foreign-owner
takeover remain unqualified until actual core support is adopted.

`onBehalfOf` can identify only the caller or its trusted direct sponsor. Sponsored
clients default to their sponsor. Grouped presence uses one actual session and
principal as its representative, the first visible focus, and the sorted union
of visible current claims. It adds no hidden-member counts. The public principal
omits caveats. Grouping conveys no session ownership or delegated authority.

`AuthorizationInstalled` is a required current host boundary. The baseline
service has no current-policy read integration; installed policy therefore
requires an explicit `ReadPolicy` port for both project presence admission and
per-bead reads. An absent port returns `authz_unavailable`; arbitrary policy
errors never become permits. Scoped principals without a plugin are refused.
No permissive policy adapter is shipped. Positive installed-policy integration
awaits the host's authoritative context/fact boundary and is not certified here.

`presence.watch` remains unbound (`method_not_found`) and absent from discovery.
No snapshot/delta stream, watch quota reservation, views, login sessions, or
interactive-human proofs are implemented. `SessionContext` is only an ownership
and cancellation seam for the future watch owner; it is not a subscription.
