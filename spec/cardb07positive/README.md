# B07 public positive routes

These independent public fixtures cover the three success routes that the
bounded B07 vocabulary/identity unit leaves open. They use the composed byte-RPC
entrypoint and actual `x.workflow.invoke` identities without an admitted source
runtime for ordinary memory, SQLite, and PostgreSQL. Memory declares a process
checkpoint lifetime; `Persistent()` remains false. SQL fixtures close and reopen
distinct handles and require a higher lease epoch.

`TestB07OrdinaryMemoryMixedProviderSuccess` and
`TestB07OrdinarySQLGenericMixedBatchSuccess/{sqlite,postgres}` submit ordered
active and durable patches and both directed mixed links through generic
`tx.apply`. They assert exact values, endpoints, kinds, lexical metadata tokens,
one joined history/namespace publication, prior receipts and unrelated-state
conservation. Their named children cover same-key replay, SQL reopen, a late true
retype after both drafts, genuine missing IDs, unsupported external active
targets, wrong project, malformed metadata, and a fresh-key duplicate with one
namespace receipt effect and no item/edge/audit/core effect. Complete namespace
envelope, sibling values and logical wrapper conservation are checked separately;
an unrelated value is seeded inside the actual workflow namespace.

`converterroundtrip.TestB07AdmittedSourceMixedLinkSuccess/{memory,sqlite,postgres}`
uses a separate admitted public archive and its real profile/commit/provider
binding. Two patches and two new mixed links execute through the public Writer.
Every source, live and reverse edge comparison uses project, physical table,
raw ID, endpoints and kind; the archive deliberately reuses raw edge IDs.
Metadata, thread and future-field JSON token text retains whitespace, escaping,
large integers, exponent spelling and explicit empty/null values. The test
requires exact unlink/relink/reverse, physical SQL reopen, byte-equal replay and
full conservation on late retype, malformed and unsupported requests. The
existing-origin metadata-change control preserves its raw edge ID, raw kind and
thread while changing exact metadata/future tokens, then replays and reverses
without further effects. Raw related-to-live relates-to bindings are archived
without changing the raw source/reverse tuple.

These implement the mixed writes, rollback, same-type re-add, retype and mixed
identity oracles in `engdocs/design/plug/spec/behaviors/work.md`. The inherited
public vocabulary/opaque/readiness controls and explicit canonical native target
remain required alongside these positives. Capability-unavailable fixtures are
separately named and receive no positive credit.

RPC requests/responses, state inventories, source bindings and reverse JSONL
are archived under `TEST_UNDECLARED_OUTPUTS_DIR/b07positive`. The state census
includes records, retained versions, visible log, all derived tables, every
checkpoint and fixture-scoped core receipts. Runtime qualification requires
named RUN/PASS leaves and matching XML without skips in the author's exact-head
focus and literal canonical whole originals, plus independent review. This
public-fixture proof conveys no native-history, admission, hosted, landing or
FullS1 credit by itself.
