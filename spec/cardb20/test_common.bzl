# Shared source and dependency order for the existing card targets.

COMMON = ["current_test.go", "postgres_test.go", "runtime_test.go", "policy_test.go", "caller_test.go", "sql_rows_test.go", "caller_closure_test.go", "vocabulary_test.go"]
DEPS = [
    "//internal/cli",
    "//internal/client",
    "//internal/core/authz",
    "//internal/plugins/authnapikey",
    "//internal/plugins/authzfixed",
    "//internal/server/authzrpc",
    "//internal/core/events",
    "//internal/core/model",
    "//internal/core/pipeline",
    "//internal/core/service",
    "//internal/server/events",
    "//internal/server/rpc",
    "//internal/server/watch",
    "//internal/server/watchhttp",
    "//internal/storage/driver",
    "//internal/storage/memory",
    "//internal/storage/postgres",
    "//internal/storage/sqlite",
]
