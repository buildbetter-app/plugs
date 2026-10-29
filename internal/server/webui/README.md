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
frame is served under `frames/<id>/` with a response CSP of `sandbox
allow-scripts`, no same-origin capability, no forms, and `connect-src 'none'`.
The host binds its narrow `postMessage` bridge to that frame's
`WindowProxy` (`event.source`), checks its exact method/project grant, and
injects project metadata before the existing same-origin RPC read. D16 methods
and credential/egress fields are refused; `tx.apply` and `plug_call` require an
explicit grant by exact name. The host caps each frame at 256 KiB per message
and 20 bridge calls per second.

The handler reuses anonymous `authn.AdmitHTTP` Host, Origin, peer and listener
checks for static requests, on a clone with an internal RPC sentinel. Actual
browser RPC calls carry `Plug-RPC: 1` and pass the normal transport admission.
Static GET/HEAD require loopback access; cookies, Authorization, remote peers,
cross-origin requests and other methods are refused. Sandboxed frame assets
accept only the browser's opaque `Origin: null` request after the same
loopback/host checks; their CSP still forbids network connections and forms.
Fetches omit credentials.
Authenticated hello responses explicitly report that browser sessions are
unconfigured. The host CSP permits only local assets, same-origin connections,
and explicitly configured same-origin frames; inline scripts/styles, object
embedding and forms are disabled. Frame responses add their own opaque-origin
`sandbox allow-scripts` CSP.
Untrusted titles, IDs, reasons and presence data are rendered as text.

## Connected slice and limits

- Home discovers methods, reads a bounded `bead.ready` page to obtain the
  current revision, then a bounded, pinned `history.query` page. It compares
  each item's revision with that browser's previous read. Only inspected items
  are remembered; continuing replaces the page and leaves unseen items unread.
  This is a comparison of current stored rows, not a deletion/change-event feed.
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
  not runtime-qualified by the bead/history fixture at this base.
- Presence reads advertised `presence.list`, shows at most five returned
  sessions and states the remainder. Missing methods show unavailable states.
  The UI opens no session and performs no writes.
- Refresh, cancellation, a 15-second read deadline, empty results and RPC
  refusals have explicit states. Live watches, interactive login, remote/CORS
  composition and epic rollups remain outside this slice. The frame bridge is
  a bounded host seam; plugin installation, digest locks and authorization
  grants remain owned by the plugin-host lane. Five-person UX validation
  against real views/watches remains unqualified.

## Verification

Run Go and browser checks through Bazel only:

```sh
bazel --ignore_all_rc_files test //internal/server/webui:webui_test \
  --jobs=2 --test_output=all --test_timeout=900 --test_arg=-test.v
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

Standalone desktop/mobile/state screenshots and actual request/response logs
are emitted to the Bazel test's `test.outputs/` directory (or undeclared-output
archive): `home-desktop.png`, `queue-mobile.png`, `changed-after-update.png`,
`loading.png`, `cancelled.png`, `timed-out.png`, `refused.png`, `seed-rpc.json`,
`fixture-update-rpc.json`, `browser-rpc.json` and `browser-console.json`.
