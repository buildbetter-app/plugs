# Executable census ratchet

`//tools/census:census_gate` validates the reviewed E2/C7 source and store
census fixture. The fixture is pinned to `beads-base`
`9d6fff760b7a314b476539198504d77ce0959656` and itemizes all 72 main and 28
ignored migration files, the final 51 table/view/scratch shapes and 279
columns, 15 Dolt/server source classes, all 31 configuration inputs named by
the Plug storage census, and all 13,139 Go test declarations. Every migration,
table, column, store/config input, and test declaration has an explicit
disposition.

The gate checks exact pinned counts, canonical source identities, migration
version paths, SHA-256 shape, table/column vocabulary, Dolt/server source
vocabulary, configuration precedence and vocabulary, test-source membership,
and non-empty disposition syntax. It also binds the canonical fixture digest,
so an edited or shortened fixture cannot pass by changing its summary counts.
The in-memory negative cases prove the required ratchet behavior: a known
schema/table/column source removed, an unknown schema or configuration input
introduced, or a disposition deleted all fail validation.

Runtime configuration values are represented as named sources without copying
their contents. YAML precedence layers, CLI/environment overrides, legacy
metadata, hooks, formula search/reference inputs, routes, and export
cross-checks are all explicit. The pinned formula source carries its digest;
Dolt/server state is a separate store-source ledger. This keeps the fixture
reviewable and avoids placing credentials or workspace data in the repository.

Run the gate with:

```sh
bazel test //tools/census:census_gate --test_output=all --lockfile_mode=error
```

The root alias is `//:census_gate`.

The standalone checker prints the canonical counts and digest:

```sh
bazel run //tools/census:census_check -- -census-fixture=tools/census/testdata/census.json
```
