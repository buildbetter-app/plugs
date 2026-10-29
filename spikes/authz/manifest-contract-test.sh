#!/usr/bin/env bash

# Focused contract checks for run-benchmark.sh. Every possible benchmark
# invocation is guarded from the start; only the exact -print-contract path
# reaches the selected real SDK. Accepted benchmark commands are explicit
# fakes and supply no database, correctness, or performance evidence.

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
runner="$script_dir/run-benchmark.sh"
manifest="$script_dir/benchmark-manifest.json"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

real_go="${AUTHZ_TEST_REAL_GO:-$(command -v go || true)}"
if [[ "$real_go" != /* && -n "${TEST_SRCDIR:-}" ]]; then
  real_go="$TEST_SRCDIR/$real_go"
fi
if [[ -z "$real_go" || ! -x "$real_go" ]]; then
  echo "manifest contract test: executable Go SDK not found: ${real_go:-<none>}" >&2
  exit 2
fi

# Install the guard before any real export, rejection, or accepted route.
# Both the explicit runner tool and bare PATH lookups hit this same guard.
fake_bin="$work_dir/fake-bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/go" <<'GUARDED_GO'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\n' "${AUTHZ_TEST_EXPORT_MODE:?}" "$*" >>"${AUTHZ_TEST_ALL_INVOCATIONS:?}"
if [[ "$#" == 3 && "$1" == run && "$2" == ./cmd/authz-spike && "$3" == -print-contract ]]; then
  case "${AUTHZ_TEST_EXPORT_MODE:?}" in
    real)
      "${AUTHZ_TEST_REAL_GO:?}" "$@"
      printf '%s\n' real >>"${AUTHZ_TEST_EXPORT_LOG:?}"
      ;;
    fail)
      echo "deliberately unavailable test exporter" >&2
      exit 95
      ;;
    invalid_json)
      printf '%s\n' 'deliberately invalid test exporter JSON'
      ;;
    *) exit 97 ;;
  esac
  exit 0
fi
if [[ "$#" == 1 && "$1" == version ]]; then
  exec "${AUTHZ_TEST_REAL_GO:?}" "$@"
fi

# Record every attempted non-export command even when the guard refuses it.
# Rejection tests require this file to remain absent.
printf '%s\n' "$*" >>"${AUTHZ_TEST_GO_ARGS:?}"
if [[ "${AUTHZ_TEST_ALLOW_FAKE_BENCHMARK:-0}" != 1 ]]; then
  echo "benchmark command blocked by contract-test guard: $*" >&2
  exit 96
fi
test "$#" = 5
test "$1" = run
test "$2" = ./cmd/authz-spike
test "$3" = "-engine=${AUTHZ_TEST_EXPECT_ENGINE:?}"
test "$4" = -iterations=20
test "$5" = "-output=${AUTHZ_TEST_OUTPUT:?}"
if [[ "$AUTHZ_TEST_EXPECT_ENGINE" != sqlite ]]; then
  test "${AUTHZ_POSTGRES_DSN:-}" = "${AUTHZ_TEST_DSN:?}"
fi
printf '%s\n' '{"stub":true}' >"$AUTHZ_TEST_OUTPUT"
GUARDED_GO
chmod +x "$fake_bin/go"
export AUTHZ_TEST_REAL_GO="$real_go" AUTHZ_GO_BIN="$fake_bin/go"
export PATH="$fake_bin:$PATH" AUTHZ_TEST_EXPORT_MODE=real AUTHZ_TEST_ALLOW_FAKE_BENCHMARK=0
export AUTHZ_TEST_ALL_INVOCATIONS="$work_dir/all-invocations.log"

# A real successful export is a prerequisite, not a late accepted-path check.
# The per-case guard log also proves a fresh real export for every mutation
# whose JSON can be parsed. Exporter/tool failure cannot satisfy that proof.
contract_json="$work_dir/executable-contract.json"
if ! (cd "$script_dir" && AUTHZ_TEST_EXPORT_LOG="$work_dir/baseline-export.log" \
  AUTHZ_TEST_GO_ARGS="$work_dir/baseline-go-args" \
  "$AUTHZ_GO_BIN" run ./cmd/authz-spike -print-contract) >"$contract_json"; then
  echo "manifest contract test: initial real executable export failed" >&2
  exit 1
fi
if ! jq -e 'type == "object"' "$contract_json" >/dev/null; then
  echo "manifest contract test: initial real executable export returned invalid JSON" >&2
  exit 1
fi
test "$(cat "$work_dir/baseline-export.log")" = real
test ! -e "$work_dir/baseline-go-args"
echo "real executable contract export succeeded using selected Go SDK"
printf '%s\t%s\t%s\n' baseline initial real >"$work_dir/receipts.tsv"

run_case() {
  local name="$1"
  local engine="$2"
  local input_manifest="$3"
  local artifact_dir="$work_dir/$name-artifacts"
  local dsn=""
  if [[ "$engine" != sqlite ]]; then
    dsn="postgres://guarded.example/$name"
  fi

  AUTHZ_TEST_EXPORT_LOG="$work_dir/$name-export.log" \
    AUTHZ_TEST_GO_ARGS="$work_dir/$name-go-args" \
    AUTHZ_TEST_EXPECT_ENGINE="$engine" AUTHZ_TEST_OUTPUT="$artifact_dir/result.json" \
    AUTHZ_TEST_DSN="$dsn" AUTHZ_POSTGRES_DSN="$dsn" \
    AUTHZ_ENGINE="$engine" AUTHZ_ITERATIONS=20 \
    AUTHZ_MANIFEST_PATH="$input_manifest" AUTHZ_ARTIFACT_DIR="$artifact_dir" \
    "$runner" >"$work_dir/$name.log" 2>&1
}

assert_export() {
  local name="$1"
  local expected="$2"
  if [[ "$expected" == none ]]; then
    test ! -e "$work_dir/$name-export.log"
  else
    test "$(cat "$work_dir/$name-export.log")" = "$expected"
  fi
}

assert_before_setup() {
  local name="$1"
  test ! -e "$work_dir/$name-artifacts" || {
    echo "case created artifacts before rejection: $name" >&2
    exit 1
  }
  test ! -e "$work_dir/$name-go-args" || {
    echo "case attempted a benchmark command before rejection: $name" >&2
    cat "$work_dir/$name-go-args" >&2
    exit 1
  }
}

expect_rejected_manifest() {
  local name="$1"
  local mutated="$2"
  local detail="$3"
  local expected_export="${4:-real}"
  local engine="${5:-sqlite}"
  local status=0

  run_case "$name" "$engine" "$mutated" || status=$?
  if [[ "$status" != 2 ]] || ! grep -Fxq "manifest contract: $detail" "$work_dir/$name.log" \
    || grep -q '^manifest exporter:' "$work_dir/$name.log"; then
    echo "case did not reject the intended declaration (exit $status): $name" >&2
    cat "$work_dir/$name.log" >&2
    exit 1
  fi
  assert_export "$name" "$expected_export"
  assert_before_setup "$name"
  printf '%s\t%s\t%s\n' "$name" rejected "$expected_export" >>"$work_dir/receipts.tsv"
  echo "rejected before setup (export=$expected_export): $name"
}

expect_rejected() {
  local name="$1"
  local filter="$2"
  local detail="${3:-matrix, dataset, targets, schema, or module diverges from executable}"
  local mutated="$work_dir/$name.json"

  jq "$filter" "$manifest" >"$mutated"
  expect_rejected_manifest "$name" "$mutated" "$detail"
}

expect_exporter_failure() {
  local name="$1"
  local detail="$2"
  local status=0

  run_case "$name" sqlite "$manifest" || status=$?
  if [[ "$status" != 3 ]] || ! grep -Fxq "manifest exporter: $detail" "$work_dir/$name.log" \
    || grep -q '^manifest contract:' "$work_dir/$name.log"; then
    echo "exporter failure was not distinguished from incompatibility (exit $status): $name" >&2
    cat "$work_dir/$name.log" >&2
    exit 1
  fi
  assert_export "$name" none
  assert_before_setup "$name"
  printf '%s\t%s\t%s\n' "$name" exporter_failure none >>"$work_dir/receipts.tsv"
  echo "exporter/tool failure distinguished before setup: $name"
}

AUTHZ_TEST_EXPORT_MODE=fail expect_exporter_failure exporter_unavailable \
  'executable contract export failed'
AUTHZ_TEST_EXPORT_MODE=invalid_json expect_exporter_failure exporter_invalid_json \
  'executable contract export returned invalid JSON'
AUTHZ_GO_BIN="$work_dir/missing-go" expect_exporter_failure missing_sdk \
  'selected Go SDK executable not found'

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
expect_rejected command '.commands.sqlite += " changed"' \
  'command declarations diverge from executable runner'
expect_rejected provisioning '.postgres_provisioning.image = "postgres:15"' \
  'Postgres provisioning declaration diverges from executable runner'
expect_rejected artifacts '.artifacts.result = "other.json"' \
  'artifact declarations diverge from executable runner'
expect_rejected extra_top_level '.unexpected = true' 'unsupported top-level declaration'

# Parsed equality must keep array order and JSON boolean/number types distinct.
for array_name in engines visibility_fractions plans queries; do
  expect_rejected "reversed_$array_name" ".matrix.$array_name |= reverse"
done
expect_rejected boolean_iterations '.matrix.measured_iterations = true'

# jq cannot preserve duplicate members. Construct these from the original bytes
# and require rejection before any export, normalization, or setup.
python3 - "$manifest" "$work_dir" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1]).read_text(encoding="utf-8")
output = Path(sys.argv[2])
schema = '  "schema": "plug-authz-benchmark/v1",'
warmups = '    "warmups": 3,'
if source.count(schema) != 1 or source.count(warmups) != 1:
    raise SystemExit("manifest declarations were not found exactly once")
for name, needle, replacement in [
    ("duplicate_schema", schema, '  "schema": "unsupported",\n' + schema),
    ("duplicate_nested", warmups, warmups + "\n" + warmups),
    ("duplicate_escaped", schema, '  "\\u0073chema": "plug-authz-benchmark/v1",\n' + schema),
]:
    (output / f"{name}.json").write_text(source.replace(needle, replacement, 1), encoding="utf-8")
PY
for duplicate in duplicate_schema duplicate_escaped; do
  expect_rejected_manifest "$duplicate" "$work_dir/$duplicate.json" \
    'invalid JSON (duplicate object member: schema)' none
done
expect_rejected_manifest duplicate_nested "$work_dir/duplicate_nested.json" \
  'invalid JSON (duplicate object member: warmups)' none

# Malformed declarations also stop before any exporter or benchmark call.
printf '%s' '{"schema":' >"$work_dir/malformed.json"
expect_rejected_manifest malformed_manifest "$work_dir/malformed.json" \
  'invalid JSON (Expecting value: line 1 column 11 (char 10))' none
printf '%s' '{} {}' >"$work_dir/multiple-documents.json"
expect_rejected_manifest multiple_documents "$work_dir/multiple-documents.json" \
  'invalid JSON (Extra data: line 1 column 4 (char 3))' none

# Change the actual executable in a disposable copy, then request a matching
# manifest. Its authentic export must still fail the runner's engine-set
# capability check. No replayed contract can stand in for this invocation.
drift_module="$work_dir/drift-checkout/spikes/authz"
mkdir -p "$drift_module"
cp -RL "$script_dir/." "$drift_module/"
python3 - "$drift_module/schema.go" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1])
contents = source.read_text()
needle = 'return []string{string(DialectSQLite), string(DialectPostgres)}'
if contents.count(needle) != 1:
    raise SystemExit("executable engine declaration was not found exactly once")
source.write_text(contents.replace(needle, 'return []string{string(DialectSQLite)}', 1))
PY
drift_manifest="$work_dir/drift-manifest.json"
jq '.matrix.engines = ["sqlite"]' "$manifest" >"$drift_manifest"
runner="$drift_module/run-benchmark.sh" \
  expect_rejected_manifest engine_set_drift "$drift_manifest" \
    'executable engine set is unsupported by this runner' real postgres

expect_accepted_engine() {
  local name="$1"
  local engine="$2"
  local accepted_manifest="$3"
  local artifact_dir="$work_dir/$name-artifacts"
  local status=0
  local expected_args="run ./cmd/authz-spike -engine=$engine -iterations=20 -output=$artifact_dir/result.json"

  AUTHZ_TEST_ALLOW_FAKE_BENCHMARK=1 run_case "$name" "$engine" "$accepted_manifest" || status=$?
  if [[ "$status" != 0 ]]; then
    echo "valid contract failed before fake benchmark route (exit $status): $name" >&2
    cat "$work_dir/$name.log" >&2
    exit 1
  fi
  assert_export "$name" real
  grep -q '^authorization benchmark complete:' "$work_dir/$name.log"
  jq -e '. == {"stub":true}' "$artifact_dir/result.json" >/dev/null
  test -s "$artifact_dir/command.txt"
  test -s "$artifact_dir/source-files.sha256"
  test "$(cat "$work_dir/$name-go-args")" = "$expected_args"
  printf '%s\t%s\t%s\n' "$name" accepted real >>"$work_dir/receipts.tsv"
  echo "valid contract reached fake $engine benchmark command: $name"
}

reordered_manifest="$work_dir/reordered.json"
jq -S . "$manifest" >"$reordered_manifest"
python3 - "$manifest" "$work_dir" <<'PY'
import json
from pathlib import Path
import sys

source = Path(sys.argv[1]).read_text(encoding="utf-8")
output = Path(sys.argv[2])
value = json.loads(source)


def reverse_objects(item):
    if isinstance(item, dict):
        return {key: reverse_objects(value) for key, value in reversed(item.items())}
    if isinstance(item, list):
        return [reverse_objects(value) for value in item]
    return item


(output / "reversed-objects.json").write_text(json.dumps(reverse_objects(value)), encoding="utf-8")
# Use raw JSON spellings: jq versions that preserve number tokens must still
# see 20.0/2e1 here rather than normalizing them while creating the fixture.
needle = '"measured_iterations": 20'
if source.count(needle) != 1:
    raise SystemExit("measured iteration declaration was not found exactly once")
(output / "decimal-iterations.json").write_text(source.replace(needle, '"measured_iterations": 20.0'), encoding="utf-8")
(output / "exponent-iterations.json").write_text(source.replace(needle, '"measured_iterations": 2e1'), encoding="utf-8")
(output / "decimal-integrals.json").write_text(json.dumps(json.loads(source, parse_int=float)), encoding="utf-8")
PY
for input_manifest in "$reordered_manifest" "$work_dir/reversed-objects.json" \
  "$work_dir/decimal-iterations.json" "$work_dir/exponent-iterations.json" \
  "$work_dir/decimal-integrals.json"; do
  for array_name in engines visibility_fractions plans queries; do
    jq -e --arg name "$array_name" --slurpfile original "$manifest" \
      '.matrix[$name] == $original[0].matrix[$name]' "$input_manifest" >/dev/null
  done
done

expect_accepted_engine valid_sqlite sqlite "$manifest"
expect_accepted_engine reordered_object sqlite "$reordered_manifest"
expect_accepted_engine reversed_objects sqlite "$work_dir/reversed-objects.json"
expect_accepted_engine decimal_iterations sqlite "$work_dir/decimal-iterations.json"
expect_accepted_engine exponent_iterations sqlite "$work_dir/exponent-iterations.json"
expect_accepted_engine decimal_integrals sqlite "$work_dir/decimal-integrals.json"
expect_accepted_engine valid_postgres postgres "$manifest"
expect_accepted_engine valid_all all "$manifest"

if [[ -n "${AUTHZ_TEST_EVIDENCE_DIR:-}" ]]; then
  mkdir -p "$AUTHZ_TEST_EVIDENCE_DIR"
  cp "$contract_json" "$work_dir/all-invocations.log" "$AUTHZ_TEST_EVIDENCE_DIR/"
  python3 - "$work_dir" "$AUTHZ_TEST_EVIDENCE_DIR/case-ledger.json" <<'PY'
import json
from pathlib import Path
import sys

work, output = map(Path, sys.argv[1:])
cases = []
for line in (work / "receipts.tsv").read_text().splitlines():
    name, outcome, export = line.split("\t")
    args = work / (name + "-go-args")
    cases.append({"name": name, "outcome": outcome,
                  "successful_real_exports": int(export == "real"),
                  "fake_benchmark_commands": args.read_text().splitlines() if args.exists() else []})
assert sum(case["successful_real_exports"] for case in cases) == 33
assert sum(case["outcome"] == "rejected" and case["successful_real_exports"] == 1 for case in cases) == 24
assert sum(case["outcome"] == "rejected" and case["successful_real_exports"] == 0 for case in cases) == 5
assert sum(len(case["fake_benchmark_commands"]) for case in cases) == 8
assert all(not case["fake_benchmark_commands"] for case in cases if case["outcome"] != "accepted")
output.write_text(json.dumps({"cases": cases, "real_benchmark_commands": 0}, indent=2) + "\n")
print("contract ledger: 33 authentic exports, 24 value rejections, 5 parsing rejections, 8 fake routes")
PY
fi
