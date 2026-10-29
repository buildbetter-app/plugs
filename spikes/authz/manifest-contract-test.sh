#!/usr/bin/env bash

# Focused contract checks for run-benchmark.sh. Mutation cases must fail before
# an artifact directory is created; the final valid case runs the bounded
# SQLite command and checks that it produced the normal result shape.

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
runner="$script_dir/run-benchmark.sh"
manifest="$script_dir/benchmark-manifest.json"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

expect_rejected() {
  local name="$1"
  local filter="$2"
  local mutated="$work_dir/$name.json"
  local artifact_dir="$work_dir/$name-artifacts"
  local log="$work_dir/$name.log"

  jq "$filter" "$manifest" >"$mutated"
  if AUTHZ_ENGINE=sqlite AUTHZ_ITERATIONS=20 AUTHZ_MANIFEST_PATH="$mutated" \
    AUTHZ_ARTIFACT_DIR="$artifact_dir" "$runner" >"$log" 2>&1; then
    echo "mutation unexpectedly accepted: $name" >&2
    cat "$log" >&2
    exit 1
  fi
  grep -q '^manifest contract:' "$log" || {
    echo "mutation was not rejected by contract validation: $name" >&2
    cat "$log" >&2
    exit 1
  }
  test ! -e "$artifact_dir" || {
    echo "mutation created artifacts before rejection: $name" >&2
    exit 1
  }
  echo "rejected before setup: $name"
}

expect_rejected schema '.schema = "plug-authz-benchmark/v2"'
expect_rejected module '.module = "spikes/other"'
expect_rejected engines '.matrix.engines = ["sqlite", "mysql"]'
expect_rejected visibility_fractions '.matrix.visibility_fractions[1] = 0.02'
expect_rejected plans '.matrix.plans[0] = "C other"'
expect_rejected queries '.matrix.queries[0] = "Q1 other"'
expect_rejected warmups '.matrix.warmups = 4'
expect_rejected measured_iterations '.matrix.measured_iterations = 19'
expect_rejected beads '.dataset.beads = 99999'
expect_rejected depth '.dataset.maximum_graph_depth = 9'
expect_rejected closure_descendants '.dataset.closure_rewrite_descendants = 49999'
expect_rejected closure_limit '.dataset.normal_write_closure_limit = 4999'
expect_rejected page_target '.targets_ms.page_p95 = 49'
expect_rejected count_target '.targets_ms.grouped_count_p95 = 249'
expect_rejected command '.commands.sqlite += " changed"'
expect_rejected provisioning '.postgres_provisioning.image = "postgres:15"'
expect_rejected artifacts '.artifacts.result = "other.json"'
expect_rejected extra_top_level '.unexpected = true'

real_go="$(command -v go)"
contract_json="$work_dir/executable-contract.json"
(cd "$script_dir" && "$real_go" run ./cmd/authz-spike -print-contract) >"$contract_json"
fake_bin="$work_dir/fake-bin"
fake_go_args="$work_dir/fake-go-args"
mkdir -p "$fake_bin"
cat >"$fake_bin/go" <<'FAKE_GO'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == run && "${2:-}" == ./cmd/authz-spike && " $* " == *" -print-contract "* ]]; then
  cat "$AUTHZ_TEST_CONTRACT"
  exit 0
fi
if [[ "${1:-}" == run && "${2:-}" == ./cmd/authz-spike ]]; then
  printf '%s\n' "$*" >"$AUTHZ_TEST_GO_ARGS"
  output=""
  for arg in "$@"; do
    case "$arg" in
      -output=*) output="${arg#-output=}" ;;
    esac
  done
  test -n "$output"
  printf '%s\n' '{"stub":true}' >"$output"
  exit 0
fi
exec "$AUTHZ_TEST_REAL_GO" "$@"
FAKE_GO
chmod +x "$fake_bin/go"

valid_artifact_dir="$work_dir/valid-artifacts"
valid_log="$work_dir/valid.log"
AUTHZ_TEST_CONTRACT="$contract_json" AUTHZ_TEST_GO_ARGS="$fake_go_args" AUTHZ_TEST_REAL_GO="$real_go" \
  PATH="$fake_bin:$PATH" AUTHZ_ENGINE=sqlite AUTHZ_ITERATIONS=20 \
  AUTHZ_ARTIFACT_DIR="$valid_artifact_dir" "$runner" >"$valid_log" 2>&1
grep -q 'authorization benchmark complete:' "$valid_log"
test -s "$valid_artifact_dir/result.json"
test -s "$valid_artifact_dir/command.txt"
test -s "$valid_artifact_dir/source-files.sha256"
expected_args="run ./cmd/authz-spike -engine=sqlite -iterations=20 -output=$valid_artifact_dir/result.json"
test "$(cat "$fake_go_args")" = "$expected_args"
echo "valid contract reached bounded SQLite benchmark command"
