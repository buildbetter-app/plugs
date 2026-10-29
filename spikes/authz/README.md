# Authorization spike

This standalone module implements the narrowed authorization spike from
`engdocs/design/plug/authorization-options.md`. It uses hand-written SQL for
plans A and B, a pure-Go SQLite driver, and a separately supplied Postgres DSN.

Run the correctness/differential tests with `go test ./...`. Run the measured
spike with:

```sh
go run ./cmd/authz-spike -engine=all -postgres-dsn "$AUTHZ_POSTGRES_DSN" -iterations=20 -output results.json
```

The default grouped-count acceptance target is p95 below 250 ms, agreed before
the run. The page target is p95 below 50 ms.

## Reproducible benchmark artifacts

The checked-in [`benchmark-manifest.json`](benchmark-manifest.json) is the
matrix contract: three visibility fractions (100%, 1%, and 0.1%), both SQL
plans, all Q4--Q6 cases, three warmups, and 20 measured iterations on each
engine. Run [`run-benchmark.sh`](run-benchmark.sh) from this module to retain
the result JSON, console output, redacted command, toolchain and commit
metadata, and SHA-256 digests:

```sh
# Set AUTHZ_POSTGRES_DSN in the calling environment; do not put its value in
# this command, a checked-in file, or an artifact.
AUTHZ_ENGINE=all AUTHZ_ITERATIONS=20 \
  AUTHZ_ARTIFACT_DIR=artifacts/all ./run-benchmark.sh
```

For a Postgres run, provision a disposable Postgres 16 instance separately
and set `AUTHZ_POSTGRES_DSN`; the runner records only that the DSN came from
the environment. The runner never writes credentials to an artifact. The
manifest includes the engine start command template and every output filename
needed to attach the run to a result table.

Before it checks a DSN, creates an artifact directory, or starts the benchmark,
the runner asks the executable for `-print-contract` and fail-closed compares
the complete supported matrix, dataset, targets, command templates, Postgres
provisioning declaration, and artifact names with the manifest. Comparisons
use parsed JSON values: object order and equivalent numeric spellings such as
`20` and `20.0` do not change the contract; array order remains meaningful.
Duplicate object member names are rejected before normalization or export.
Set `AUTHZ_GO_BIN` to select a Go executable explicitly; the runner uses that
tool for contract export, version metadata, and benchmark execution. Invalid
declarations exit 2 with `manifest contract:`; unavailable tools or failed
exports exit 3 with `manifest exporter:`.

Focused coverage is available from
[`manifest-contract-test.sh`](manifest-contract-test.sh). It installs a
benchmark guard before the first invocation, requires an initial successful
real executable export, and requires each parseable mutation to have its own
successful real export and the intended rejection reason. It also tests
exporter failure, object/numeric equivalence, meaningful array order,
duplicate rejection, and engine routing. Accepted benchmark commands are
explicit fakes that write only a stub result; they prove no performance or
database behavior. A separate fake exporter checks matching engine-set drift.

The same executable contract and guarded harness run under Bazel with
`bazel test //spikes/authz:manifest_contract_test --config=ci --cache_test_results=no --test_output=all`.
Every real export delegates to the registered Go SDK. The nested module still
needs a populated module cache or runtime dependency downloads; those modules
are not yet declared Bazel inputs. Repository inventory/guard integration for
this package also remains pending. This target alone does not establish
portable offline acceptance or continuing repository-guard coverage.

The JSON `CandidateRows` field is measured SQL cardinality evidence for the
rows a plan must qualify or walk: scope-first counts its qualified ready rows,
while index-walk counts ready rows through the returned key (or all ready rows
for grouped counts). It is named and reported as candidate cardinality, not as
an engine-level executor counter; `database/sql` does not expose one portable
row-loop counter across the pure-Go SQLite and Postgres drivers used here.

## Published v6 evidence

The repaired run's concise [v6 result readout](evidence-v6/readout.md),
structured SQLite/PostgreSQL outputs, and credential-free
[provenance manifest](evidence-v6/provenance-manifest.json) are committed with
the spike. The readout records the measured source commit separately from the
later evidence-only publication commit; raw logs and DSNs are not committed.
