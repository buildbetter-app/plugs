# Additional recorded behavior references

This package is an independent extension to the first recorded-reference
package. It records the B07 same-type edge re-add and remove/re-add slice,
and the B25 scalar-update relation preservation slice. The
existing B17/B24 package is deliberately unchanged.

The vectors were produced twice from the `beads-base` tag at
`9d6fff760b7a314b476539198504d77ce0959656` with the pinned embedded engine.
Each run uses a fresh disposable workspace, explicit IDs, fixed titles and
priorities, fixed actors and owner, and an explicit environment. The selected
operations do not use clock predicates. Runtime Go/SQL timestamps, revision
hashes and generated relation IDs are explicitly omitted; these vectors are
not controlled-time certificates. Array order is preserved exactly, including
the comment thread. A failed command or changed relation prevents emission.

The retained source references point to the pinned dependency and preservation
protocol tests. `build50_record.py` is the executable recorder and `provenance.py`
independently canonicalizes each raw artifact, derives the declared projection,
and binds the source, driver, schema, clock, configuration, and input digests.
The two recorder artifacts and two provenance outputs for every card must be
byte-identical. The projector checks every source blob against the frozen tag,
the source-test symbols, driver version and both migration inventories. Schema
cursors are inventory evidence; the CLI does not expose embedded SQL queries.

The manifest also binds the exact reference source tree and Go/Python toolchain,
binary build-info digest, recorder and projector source bytes, source-test paths,
both execution command pairs, and the raw and semantic digest pairs. Its validator
rejects deleted provenance and altered evidence even when that evidence is
rehashed. The existing immutable-reference admission digest is unchanged; the
extension and its validator remain subject to the full lexicon scan.

`//spec:acceptance` consumes the actual vectors through the existing deterministic
transport, RPC service, Writer and memory driver. Graph setup uses the real
Writer because dependency RPC binding remains owned by #101. No handler or
store substitute is used. These are slice-level `keep` results, not full-card
certificates: B07's conflicting retype, custom kinds, cross-plane graphs and
blocker semantics, and B25's deletes/cascades and remaining scalar/relation
combinations are outside this extension. SQLite/Postgres replay remains open.
The full recorded catalog is still incomplete.

Each successful detail-read projection is compared to the retained reference
observations, including edge kind/endpoints, label order, comment body order/count,
and the final scalar fields. Two fresh Plug stacks must also produce the same
RPC frame bytes. Raw RPC frames and observed JSON are saved in Bazel's undeclared
test outputs, including on a comparison failure. This runner translates the
selected operation vocabulary; it does not execute or certify the complete
upstream protocol tests named as source references.

To repeat a reference recording, build the pinned source using the manifest's
`provenance.binary.buildCommand`, then run each card's two `commands` and two
`projectionCommands`. They use independent disposable stores. The optional
`BUILD50_AUDIT_DIR` retains every reference command's unprojected stdout/stderr,
arguments, environment and workspace separately from the byte-stable artifact.
No production store or inherited application configuration is used.

Run the focused check with:

```sh
bazel --ignore_all_rc_files test //spec/recorded/extension:extension_test \
  //spec:acceptance --disk_cache=/tmp/plug-swarm/bazel-disk-cache \
  --local_resources=memory=8192 --jobs=2 --test_timeout=900 \
  --test_output=all --cache_test_results=no --lockfile_mode=error
```
