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
provisioning declaration, and artifact names with the manifest. Focused
mutation coverage is available from
[`manifest-contract-test.sh`](manifest-contract-test.sh); it rejects altered
declarations before setup and exercises a valid bounded command path.

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
