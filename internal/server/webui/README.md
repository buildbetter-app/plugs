# Embedded shared-server UI

`webui.New(Config)` returns a read-only `http.Handler` with embedded HTML,
CSS, JavaScript and favicon. It owns no listener, RPC registry, store, or
session provider. It adds no command or production server composition.

Mount it beside an **already admitted** anonymous RPC handler. For example,
inside a composition root that has constructed `admittedRPC` with
`httpjson.New`:

```go
ui, err := webui.New(webui.Config{
    MountPath: "/ui/",
    RPCPath:   httpjson.Path,
    Project:   "work",
    MaxRows:   25,
})
if err != nil {
    return err
}
mux := http.NewServeMux()
mux.Handle(httpjson.Path, admittedRPC)
mux.Handle("/ui/", ui)
// Serve mux on the composition's anonymous loopback listener.
```

`MountPath` defaults to `/ui/`, `RPCPath` to `/rpc/v1`, `Project` to
`default`, and `MaxRows` to 25 (allowed range 1–100). Paths must be clean,
absolute URL paths; the mount ends in `/` and the RPC path does not.
`config.json` exposes this explicit composition to the browser.

`Catalog` supplies validated declarative panels, columns, and buttons. The
default catalog records the chosen "Changes since I looked" home, `blockedBy`
column, and `bead.claimNext` action as data; it does not execute plugin code in
the host page. `Frames` optionally supplies prebuilt embedded assets. Each
`FrameBundle` requires a pinned lowercase SHA-256 `Digest` over its complete
asset inventory. `FrameBundleDigest` computes the packaging pin: JSON with
format `plug-ui-bundle/1` and path-sorted files containing `path`, byte `size`,
and lowercase `sha256`. Admission verifies a detached copy against the supplied
pin; missing pins, extra/missing files and changed paths or bytes refuse startup.
It never refreshes a pin or builds JavaScript during admission.

Each frame is served under `frames/<id>/` with a response CSP of `sandbox
allow-scripts`, no same-origin capability, no forms, and `connect-src 'none'`.
Script, style and image sources are limited to that bundle's own URL directory.
Each plugin is nested inside a trusted, same-origin host wrapper. The wrapper's
`frame-src` admits only that plugin's exact `index.html` URL; it contains every
child navigation, including other same-origin bundles, before another document
can run. This uses implemented frame-source enforcement, not `navigate-to` or a
load-event race. The wrapper never evaluates plugin data, and the opaque child
cannot navigate or inspect its trusted ancestor. Plugin assets stay unchanged.
The wrapper binds the child's `WindowProxy` and opaque origin; the main host
binds the trusted wrapper's `event.source` and exact host origin. It checks the
same narrow `postMessage` method/project grant and
injects project metadata before the existing same-origin RPC read. D16 methods
and credential/egress fields are refused; `tx.apply` and `plug_call` require an
explicit grant by exact name. The host caps each frame at 256 KiB per message
and 20 bridge calls per second.
Navigation-policy violations, reloads, pagehide and removal permanently revoke
the bridge and abort pending host calls; replies are suppressed after revocation.
Reloading the host creates a new contained frame and a fresh grant.

Before stamping metadata, the live browser bridge checks every explicit
`project` and `projects` value recursively through objects and arrays. A
different project or malformed project value returns `scope_denied` without
sending an RPC request. Omitted project metadata is supplied by the host grant.

The handler reuses anonymous `authn.AdmitHTTP` Host, Origin, peer and listener
checks for static requests, on a clone with an internal RPC sentinel. Actual
browser RPC calls carry `Plug-RPC: 1` and pass the normal transport admission.
Static GET/HEAD require loopback access; cookies, Authorization, remote peers,
cross-origin requests and other methods are refused. Sandboxed frame assets
accept only the browser's opaque `Origin: null` request after the same
loopback/host checks; their CSP still forbids network connections and forms.
These public, digest-checked frame assets use cross-origin resource admission
so an opaque frame can load its actual external scripts. Module requests from
`Origin: null` receive only `Access-Control-Allow-Origin: null`, with no
credential allowance. Host assets retain same-origin resource policy. Direct
opens retain the sandbox, and credentialed asset requests still refuse.
Fetches omit credentials.
Authenticated hello responses explicitly report that browser sessions are
unconfigured. The host CSP permits only local assets, same-origin connections,
and explicitly configured same-origin frames; inline scripts/styles, object
embedding and forms are disabled. Frame responses add their own opaque-origin
`sandbox allow-scripts` CSP.
Untrusted titles, IDs, reasons and presence data are rendered as text.

When native RPC reports authenticated mode, the page offers explicit bearer
sign-in using a password field. The key and authenticated read markers stay in
tab memory, not URLs, browser storage, cookies, static requests or plugin frames.
Sign-out, identity changes and current-key authentication failures clear the
credential, read markers, displayed rows, work, watches and frame bridges. This
includes native 401s encountered only while stopping live changes; no refresh is
needed to clear an expired or revoked identity. Non-authentication refusals do
not sign the caller out.

Each collaboration opening retains its originating credential generation and
same-origin RPC endpoint. If sign-out or an identity change happens before an
opening reply arrives, the late session/subscription is released with that
opening identity, rather than the empty or replacement bearer. A retired key is
used only for that bounded allocation cleanup, never a new read or watch, and
its late authentication failures cannot clear a replacement identity. This
bearer flow is not an OIDC or browser-session login composition.

`sandbox_safety_test` runs Chrome against the real UI and RPC handler. The
bundle's classic and module scripts must load under the product headers. It
checks frame and direct-open storage/DOM isolation, denied network/forms/popups,
own-directory script loading, source-bound grants, and RPC credential omission
with a real browser cookie present. The OIDC integration browser test separately
proves actual CORS preflights and origin-bound bearer calls without credentialed
CORS. Both use disposable loopback fixtures, not a production server.

## Connected slice and limits

- Home discovers methods, reads a bounded `bead.ready` page to obtain the
  current revision, then a bounded, pinned `history.query` page. It compares
  each item's revision with that browser's previous read. Only inspected items
  are remembered; continuing replaces the page and leaves unseen items unread.
  This is a comparison of current stored rows, not a deletion/change-event feed.
  This legacy path is used only when `view.run` is absent and no `HomeViewID`
  is configured; it does not qualify view/watch behavior.
- With advertised `view.run`, Home renders actual stored view rows. Optional
  `HomeViewID` selects a saved definition whose server-owned `budgetTokens`
  must be at most `MaxRows`. Without an ID, the browser sends a bounded inline
  definition with project supplied through the existing RPC metadata. A real
  view refusal never falls back to ready/history. Continuations carry the
  signed cursor and exact `asOf`; every page replaces the previous page.
- Optional `WatchPath` selects the existing same-origin NDJSON endpoint and
  requires `HomeViewID`. With advertised `view.watch`, `event.unsubscribe`,
  `session.open` and `session.close`, the browser opens an ephemeral
  collaboration lifetime, subscribes before its first view read, and attaches
  using `Plug-Session`. Session IDs stay in memory and never enter frame calls.
  Canonical snapshot/delta frames trigger bounded `view.run` rereads, preserving
  the relative order of surviving rows. Event bodies never manufacture rows.
  The first page remains capped; paging stops the watch and pins the snapshot.
  Stop, navigation, page exit, cancellation and read failures abort delivery,
  unsubscribe and close the actual session. Reset, gap, visibility loss,
  invalid cursor chains or stream failure clear stale rows and continuation;
  reconnect requires Refresh. The initial omitted-cursor open has one narrow
  bootstrap: a first terminal `ontology_changed` frame with matching opaque
  cursor/subscription fields closes that stream and reopens once from its
  supplied cursor, retaining the session before the normal current snapshot.
  Later ontology changes and all other reset reasons still require Refresh.
  A late opening allocation is also released.
- The local read map is scoped by endpoint, project and snapshot incarnation,
  and retains at most 1,000 items. Eviction or unavailable browser storage can
  show a previously read item again. It never creates a server checkpoint.
- What next reads live `bead.ready` cursors in server order. Counts describe
  returned pages, not project totals. A continuation says more rows exist;
  the current bead/history API supplies no omitted-row count.
- Blocked rows require advertised `view.run` and an operator-supplied
  `BlockedViewID` or canonical `BlockedViewDefinition`. Explanation requires
  advertised `view.explain` and a saved view ID. Grouped view results and
  pages exceeding the row budget explicitly refuse. These optional calls are
  not runtime-qualified by the legacy bead/history fixture.
- Presence reads advertised `presence.list`, shows at most five returned
  sessions and states the remainder. It renders the returned principal
  kind/id, delegation, focus, bounded working-on resources, and optional
  `displayName`. The label is explicitly marked untrusted and rendered as text
  beside the principal, separate from `onBehalfOf`; it never replaces either.
  Sessions remain separate rows even when their principal or label matches.
  Anonymous rows without a label retain the unavailable-label message; the UI
  never derives identity from an input agent label or exposes session tokens.
  Malformed label output (including null, empty or padded strings, Unicode
  Cc/Cf or surrogate code points, or more than 64 Unicode scalar values) refuses
  the read with `invalid_response`. Raw Unicode White_Space trimming,
  empty-to-absent normalization and typed `invalid_params` belong to the
  separately owned `session.open` consumer. Missing methods show
  unavailable states. The UI makes no durable writes. Live view delivery opens
  only the optional collaboration lifetime described above; this is not a
  login/auth session UI.
- Explain reads advertised `view.explain` and renders the returned reason
  strings, unreadable-blocker count, and pending-readiness flag. If the server
  returns only the `included` marker or no reason, the UI says that no direct
  reason was supplied instead of guessing a blocker.
  When optional `directBlockers` is present, each returned canonical reference
  stays beside its own safe reason and satisfied/unsatisfied/unknown verdict.
  Current branch-zero IDs link into the existing Explain item selection;
  unresolved keys, other branches, and IDs that the current form/backend
  cannot preserve unchanged retain their exact identity as text.
  Opening a link rereads the current explanation. An absent capability,
  evaluated empty result, and count-only unreadable result remain distinct.
  References and reasons use textContent. `TestBrowserDirectBlockerContractFixtures`
  covers these states and hostile text using explicit response fixtures;
  it does not qualify the producer, authorization, or full E3 workload.
- Epic progress uses only the explicitly opted-in parent-aware `view.run`
  definition: `filter.query="parent=<bare ID>"`, `fields=[]` and
  `groupBy=["epic-progress"]`. Ordinary `view.run` discovery is insufficient:
  a disabled producer refuses the definition. The tab renders visible counts
  from the `total` and `status:<canonical status>` groups, preserving the
  producer's status semantics instead of classifying statuses in the browser.
  Only the total group's `authorization` elision supplies the exact
  “N more you cannot see” remainder. Its same-request `continueWith` is a
  fresh permission check, never a hidden-child next page; the UI validates
  its scope and never executes a response-supplied call. Refresh builds the
  same parent request again without a cursor or `asOf`.
  Count-only results must be complete with no payload items or `nextCursor`.
  Unknown groups, non-authorization elisions, inconsistent totals, unsafe
  counts, more than 20,000 visible children or too many UI status rows refuse
  the entire read. The authorized candidate cap does not limit the exact
  hidden remainder, which must still be a safe natural number. No count is approximated or truncated. The request leaves
  the producer's token budget at its default; `MaxRows` is only a presentation
  bound. The producer's 100-group ceiling includes `total`, so at most 99
  visible status buckets fit in one accepted response. Snapshot status keys
  come from that one result, without an extra ontology read. Type summaries are never shown as epic progress.
  Previous counts clear before refresh, cancellation, navigation or refusal;
  refusal details never render a parent identity from a server hint.
  `TestBrowserEpicProgressPresentation` checks
  visible-only counts, the exact hidden remainder, output bounds, text-only
  status labels and mobile wrapping using explicit presentation inputs. This
  test does not qualify the hierarchy, permissions, ontology or RPC producer.
  `TestBrowserEpicProgressWireContract` separately drives the real reader
  against explicit wire fixtures, including malformed payloads, foreign
  refresh scopes, typed refusals and the 100/101-group boundary. Its hidden
  counts certify consumer behavior only.
  `TestBrowserEpicProgressStoredGraph` uses actual memory and SQLite stores,
  the Writer's parent hierarchy and status mutations, a stored protected
  ontology source, the opted-in `view.run` producer, admitted HTTP RPC and
  Chrome. It checks direct children across projects, excludes grandchildren
  and unrelated rows, rereads committed status/reparent changes and clears
  results above the presentation cap. This is an anonymous host without an
  authorization plugin; no result is fabricated. It does not qualify hidden
  child authorization or an authenticated browser. Those remain separate
  from the producer's fixed-policy HTTP acceptance and the EP6 workload.

- Refresh, cancellation, a 15-second read deadline, empty results and RPC
  refusals have explicit states. OIDC/browser-session login and remote/CORS
  composition remain outside this slice. The frame bridge is a bounded host seam;
  plugin installation, digest locks and authorization grants remain owned by
  the plugin-host lane. Five-person UX validation against real views/watches
  remains unqualified.

`TestBrowserAdmittedViewWatchStoredRowsAndCleanup` retains the original
legacy nil-event-plan journey. `TestBrowserRealExplainAndPresenceUXGaps`
adds a separate real store/Writer/RPC/Chrome journey with the admitted audited
Writer so the actual claim has committed slot provenance. Its canonical audit
adapter has no conditional-edge source locator and will refuse those decisions;
the fixture creates no such edges. It proves returned filter reasons, the
missing-direct-reason message, and actual principal/focus/working-on rendering
with no session token used as identity. Neither fixture certifies a readable
direct blocker, distinguishable anonymous labels, epic counts, or installed
production policy.

The executable composition at this change's pinned base still does not bind
`viewrpc.BindWithConfig`, supply a production `viewops.ViewReader`, or set
`watchhttp.Config.Views`; it also selects `coreevents.AllowAll` and has no
installed current read-policy adapter. Its owner must wire those existing
ports, the actual collaboration `SessionBridge`/stream admission, and the UI's
`HomeViewID`/`WatchPath` before claiming a connected executable. Watch membership
currently compiles only id/type/resources, and refuses project/status/grouped
filters. The project on a view read remains authoritative. Parent/assignee
projection and parent relations are absent; type counts cannot qualify epic
progress. No foreign composition/policy implementation is added here.

`TestBrowserPresenceDisplayNameContractFixtures` separately drives the actual
embedded page in Chrome using explicit canonical/malformed RPC response
fixtures. It proves label omission, untrusted text rendering, principal and
sponsor separation, distinct same-principal sessions, Unicode output bounds,
five-row and working-on limits, mobile wrapping, and stale-label removal after
empty/refused rereads. These fixtures do not implement or qualify the collab
consumer, real session normalization, policy masking, expiry, or the E3
20,000-bead/50-agent workload. At this fixed input the collab display-name
consumer remains a separate prerequisite for real composed qualification.

## Verification

Run Go and browser checks through Bazel only:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD webui -- test //internal/server/webui:webui_test \
  //internal/server/webui:presence_display_test \
  //internal/server/webui:epic_progress_test \
  --cache_test_results=no --test_output=all --test_arg=-test.v
```

The local test inherits PATH and requires Node 24+ (built-in WebSocket) and
`google-chrome`; missing tools fail rather than skip. It starts a disposable
memory store, real pipeline/service/registry and admitted HTTP transport,
seeds three stored beads through RPC, then drives Chrome over CDP. A separate
harness writer updates a bead through RPC, and the UI detects the new revision.
The test proves bounded paging, unread tracking, unsafe-title text rendering,
loading, empty states, unavailable methods, cancellation/deadline propagation
to a held real driver read, and refusal after actual lease release. It checks
browser RPC methods are reads and the store contains only the four harness
writes. No product read response is fabricated.

`TestBrowserAdmittedViewWatchStoredRowsAndCleanup` separately drives actual
`viewops`, `viewrpc`, collaboration and `watchhttp` over the admitted HTTP host.
Its saved-view port reads Writer-persisted metadata via the real service. Its
fixture-only current policy checks explicit stored readership grants, denies
missing grants/other projects, and exercises real grant revocation; it supplies
no blanket permit and is not a production authorization adapter. Chrome proves
stored rows, a real delta without manual refresh, pinned paging across a write,
membership reset, empty results, missing/unsupported/oversized refusals, inline
reads, text rendering, mobile width and session/subscription/stream cleanup.
Raw RPC/NDJSON, external-writer receipts and screenshots are retained under
`test.outputs/view-watch/`. This package fixture is not the independent E3
20,000-bead/50-agent/five-task workload, installed policy qualification or a
five-human study; those historical/remaining boundaries stay explicit.

The same browser fixture creates two opaque frames and uses their Chrome
debugging contexts to send real `postMessage` calls, preserving all CSP and
sandbox settings. Cross-project and malformed-scope messages stop at the live
bridge; calls with matching or omitted metadata reach real RPC and return a
stored row, while the sibling frame cannot borrow the other frame's method
grant. Responses and the RPC boundary evidence are recorded in `bridge-scope.json`.

Standalone desktop/mobile/state screenshots and actual request/response logs
are emitted to the Bazel test's `test.outputs/` directory (or undeclared-output
archive): `home-desktop.png`, `queue-mobile.png`, `changed-after-update.png`,
`loading.png`, `cancelled.png`, `timed-out.png`, `refused.png`, `seed-rpc.json`,
`fixture-update-rpc.json`, `browser-rpc.json` and `browser-console.json`.
