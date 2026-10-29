#!/usr/bin/env bash

# Focused contract checks for run-benchmark.sh. Mutation cases must fail before
# an artifact directory is created; accepted cases use a guarded fake command
# so no database, corpus, or expensive benchmark can run.

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
runner="$script_dir/run-benchmark.sh"
manifest="$script_dir/benchmark-manifest.json"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

expect_rejected_manifest() {
  local name="$1"
  local mutated="$2"
  local detail="${3:-}"
  local artifact_dir="$work_dir/$name-artifacts"
  local log="$work_dir/$name.log"

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
  if [[ -n "$detail" ]]; then
    grep -Fq "$detail" "$log" || {
      echo "rejection did not include expected detail: $name" >&2
      cat "$log" >&2
      exit 1
    }
  fi
  test ! -e "$artifact_dir" || {
    echo "mutation created artifacts before rejection: $name" >&2
    exit 1
  }
  echo "rejected before setup: $name"
}

expect_rejected() {
  local name="$1"
  local filter="$2"
  local mutated="$work_dir/$name.json"

  jq "$filter" "$manifest" >"$mutated"
  expect_rejected_manifest "$name" "$mutated"
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

# jq cannot preserve duplicate members, so construct this regression from the
# original bytes and require the runner's strict pre-normalization validator
# to identify the duplicated member by name.
duplicate_manifest="$work_dir/duplicate-schema.json"
python3 - "$manifest" "$duplicate_manifest" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1]).read_text(encoding="utf-8")
needle = '  "schema": "plug-authz-benchmark/v1",'
if source.count(needle) != 1:
    raise SystemExit("manifest schema declaration was not found exactly once")
Path(sys.argv[2]).write_text(
    source.replace(needle, '  "schema": "unsupported",\n' + needle, 1),
    encoding="utf-8",
)
PY
expect_rejected_manifest duplicate_schema "$duplicate_manifest" "duplicate object member: schema"

real_go="${AUTHZ_TEST_REAL_GO:-$(command -v go)}"
if [[ "$real_go" != /* && -n "${TEST_SRCDIR:-}" ]]; then
  real_go="$TEST_SRCDIR/$real_go"
fi
test -x "$real_go" || {
  echo "manifest contract test: executable Go tool not found: $real_go" >&2
  exit 2
}
contract_json="$work_dir/executable-contract.json"
(cd "$script_dir" && "$real_go" run ./cmd/authz-spike -print-contract) >"$contract_json"
fake_bin="$work_dir/fake-bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/go" <<'FAKE_GO'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == run && "${2:-}" == ./cmd/authz-spike && " $* " == *" -print-contract "* ]]; then
  cat "$AUTHZ_TEST_CONTRACT"
  exit 0
fi
if [[ "${1:-}" == run && "${2:-}" == ./cmd/authz-spike ]]; then
  engine=""
  iterations=""
  printf '%s\n' "$*" >"$AUTHZ_TEST_GO_ARGS"
  output=""
  for arg in "$@"; do
    case "$arg" in
      -engine=*) engine="${arg#-engine=}" ;;
      -iterations=*) iterations="${arg#-iterations=}" ;;
      -output=*) output="${arg#-output=}" ;;
    esac
  done
  test "$engine" = "${AUTHZ_TEST_EXPECT_ENGINE:?}"
  test "$iterations" = 20
  if [[ "$engine" != sqlite ]]; then
    test "${AUTHZ_POSTGRES_DSN:-}" = "${AUTHZ_TEST_DSN:?}"
  fi
  test -n "$output"
  printf '%s\n' '{"stub":true}' >"$output"
  exit 0
fi
echo "unexpected Go invocation: $*" >&2
exit 97
FAKE_GO
chmod +x "$fake_bin/go"

expect_accepted_engine() {
  local name="$1"
  local engine="$2"
  local accepted_manifest="$3"
  local artifact_dir="$work_dir/$name-artifacts"
  local log="$work_dir/$name.log"
  local fake_go_args="$work_dir/$name-go-args"
  local dsn="postgres://guarded.example/$name"

  if [[ "$engine" == sqlite ]]; then
    AUTHZ_TEST_CONTRACT="$contract_json" AUTHZ_TEST_GO_ARGS="$fake_go_args" \
      AUTHZ_TEST_REAL_GO="$real_go" AUTHZ_TEST_EXPECT_ENGINE="$engine" \
      PATH="$fake_bin:$PATH" AUTHZ_ENGINE="$engine" AUTHZ_ITERATIONS=20 \
      AUTHZ_MANIFEST_PATH="$accepted_manifest" AUTHZ_ARTIFACT_DIR="$artifact_dir" \
      "$runner" >"$log" 2>&1
  else
    AUTHZ_TEST_CONTRACT="$contract_json" AUTHZ_TEST_GO_ARGS="$fake_go_args" \
      AUTHZ_TEST_REAL_GO="$real_go" AUTHZ_TEST_EXPECT_ENGINE="$engine" \
      AUTHZ_TEST_DSN="$dsn" AUTHZ_POSTGRES_DSN="$dsn" \
      PATH="$fake_bin:$PATH" AUTHZ_ENGINE="$engine" AUTHZ_ITERATIONS=20 \
      AUTHZ_MANIFEST_PATH="$accepted_manifest" AUTHZ_ARTIFACT_DIR="$artifact_dir" \
      "$runner" >"$log" 2>&1
  fi
  grep -q 'authorization benchmark complete:' "$log"
  test -s "$artifact_dir/result.json"
  test -s "$artifact_dir/command.txt"
  test -s "$artifact_dir/source-files.sha256"
  expected_args="run ./cmd/authz-spike -engine=$engine -iterations=20 -output=$artifact_dir/result.json"
  test "$(cat "$fake_go_args")" = "$expected_args"
  echo "valid contract reached bounded $engine benchmark command"
}

reordered_manifest="$work_dir/reordered.json"
jq -S . "$manifest" >"$reordered_manifest"
for array_path in \
  '.matrix.engines' '.matrix.visibility_fractions' '.matrix.plans' '.matrix.queries'; do
  test "$(jq -c "$array_path" "$reordered_manifest")" = "$(jq -c "$array_path" "$manifest")"
done

expect_accepted_engine valid_sqlite sqlite "$manifest"
expect_accepted_engine reordered_object sqlite "$reordered_manifest"
expect_accepted_engine valid_postgres postgres "$manifest"
expect_accepted_engine valid_all all "$manifest"
