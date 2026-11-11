#!/usr/bin/env bash
set -euo pipefail
export PATH="/tmp/plug-swarm/bin/worker-safe:$PATH"
root="$TEST_SRCDIR/$TEST_WORKSPACE"
output="$TEST_UNDECLARED_OUTPUTS_DIR/generated"
mkdir -p "$output/engdocs/design/plug/contracts" "$output/internal/api/wire"
cp "$root/engdocs/design/plug/contracts/openrpc.json" "$output/engdocs/design/plug/contracts/openrpc.json"
cp "$root/internal/api/wire/catalog.json" "$output/internal/api/wire/catalog.json"
cp "$root/internal/api/wire/wire_test.go" "$output/internal/api/wire/wire_test.go"
python3 "$root/scripts/ci/reconcile-openrpc.py" --root "$output" --write
