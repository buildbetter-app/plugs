# Shared-server web acceptance

This package qualifies the admitted `internal/server/webui` handler through a
real HTTP composition with the service, Writer, RPC registry, JSON transport,
and disposable SQL stores. SQLite runs in its file-backed WAL mode; the second
matrix case uses PostgreSQL 17.11 from the pinned local image. The test never
replaces the RPC or web handler with a fixture implementation.

The HTTP checks cover embedded assets and content types, no-store and security
headers, mount/RPC route ownership, read-only method refusal, traversal and
credential/cross-origin refusal, actual row creation through JSON-RPC, pinned
history reads, and an empty project. The supplemental Chrome CDP check uses an
installed browser, renders a real row as text, creates another row through the
same RPC endpoint outside the page, verifies it after a UI refresh, visits the
real empty state, and surfaces lease-loss refusal in the DOM. It records the
browser RPC requests and screenshots when
`PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR` is set.

The `/fixture/fence` endpoint exists only in the disposable test mux. It
releases the real store lease so the page can demonstrate the product's actual
`busy` refusal; it is not a production route and does not synthesize an RPC
outcome. Browser product requests are asserted to remain reads (`server.hello`,
`rpc.methods`, `bead.ready`, and `history.query`).

Run the bounded target with:

```sh
bazel --ignore_all_rc_files test //internal/integration/webacceptance:webacceptance_test \
  --disk_cache=/tmp/plug-swarm/bazel-disk-cache --local_resources=memory=8192 \
  --jobs=2 --test_output=all --test_timeout=900
```

This is a focused web/server acceptance slice. It does not claim full A7
coverage, authentication-provider or authorization semantics, sessions,
cookies/CORS UX, watches, CLI/HA behavior, performance, crash recovery,
production listener topology, or deployment readiness.

## E3 presence adapter preparation

`presence_dom_adapter.mjs` is an acceptance-only observation adapter for the
local web UI DOM reference `cbeeb1e52c639cf3823b4cb76a083359e08cf5eb`. This
reference is not an admitted runtime dependency. No consumer source is copied
or merged here. The fixed admitted contract anchor is
`fef6e64de87826a6a20f245f298936457212897e`.

`extractPresenceDOM(container)` reads `.presence-name`, `.presence-identity`,
`.presence-display-name`, `.presence-focus`, `.presence-scope`, and `.elision`
through `textContent`. A later authorized CDP driver can evaluate this
self-contained function on `#presence-rows` and return the plain snapshot.
`comparePresence({snapshot, presence, sessions})` compares that snapshot with
actual `presence.list` rows and the expected mapping from actual opened
sessions/claims plus their planned normalized display names. It checks the
literal `Client label (untrusted): ` prefix, optional-label absence, separate
principal/delegation, resources, server order, five-row session bound and
three-resource bound with explicit remainders. Equal principals and equal
labels remain distinct sessions keyed by `sessionId`. The DOM does not expose
session IDs; visible row matching uses server order and never claims a
separately visible session token. Returned issues name the missing or
mismatched field. `matches` describes only this comparison, not qualification.

`checkE3Dependencies(pins)` emits machine-readable `BLOCKED_MISSING_ADMISSION`
when the fixed contract anchor, admitted collab/webUI head plus admission
references, or combined source head/tree plus admission reference is absent
or malformed. Complete declarations produce `DECLARED_PINS_PRESENT`, with
`admissionVerified=false` and `runtimeStatus=UNRUN`. The function cannot verify
controller admission or authorize execution. A local source/reference hash
cannot replace an admitted dependency. Controller verification and a separate
runtime intake remain required.

The unit fixtures are artificial non-secret selector ports, wire rows and
session references. They start no handler, browser or database. The separate
`presence_adapter_test` target keeps them out of the historical SQL/browser
`TestMain`; all pre-existing integration sources and assertions are unchanged.
Run the preparation unit target with installed Node and Bazel 9.3.0:

```sh
env -u CODEX_LB_API_KEY bazel --ignore_all_rc_files test \
  //internal/integration/webacceptance:presence_adapter_test \
  --disk_cache=/tmp/plug-swarm/bazel-disk-cache --local_resources=memory=8192 \
  --jobs=2 --test_timeout=900 --cache_test_results=no --test_output=all
```

The original real 20,000-bead/50-session recipe and all five task assertions
(what next, why blocked, what changed, who is on what, epic progress) remain
unchanged at acceptance head `8690ebb5f15f53034e67f5f811556192429c03a9`.
That head contains unadmitted browser source and is not imported at this fixed
base. Its safe preparation report is
`/tmp/plug-swarm/reports/E3-POST156-SCRIPTED-RUN-PREPARATION-20261030.md`.
The later combined E3 harness must retain its real Writer/driver/SQL counts,
50 actual distinct session/name/work mappings, all five task outcomes/timings,
policy/revocation and cleanup assertions, and every historical block. This
adapter does not weaken or replace those assertions. Required runtime metadata
still names 20,000 stored beads, 50 distinct sessions and all five tasks.
Full E3, installed policy, hosted CI, controller admission and human acceptance
remain unqualified; overall LOCAL BLOCK/security UNKNOWN are unchanged.
