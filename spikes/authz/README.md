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
