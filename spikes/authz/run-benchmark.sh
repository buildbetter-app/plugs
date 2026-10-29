#!/usr/bin/env bash

# Run the authorization spike with the checked-in matrix and retain enough
# metadata to rerun and audit the exact invocation.  The Postgres DSN is read
# only from AUTHZ_POSTGRES_DSN and is never copied into an artifact.

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
repo_root="$(CDPATH= cd -- "$script_dir/../.." && pwd -P)"
manifest="${AUTHZ_MANIFEST_PATH:-$script_dir/benchmark-manifest.json}"

# These names are part of the executable artifact contract. Keep the values
# in one place so validation and the files actually written cannot drift.
artifact_result_name="result.json"
artifact_stdout_name="stdout.log"
artifact_command_name="command.txt"
artifact_environment_name="environment.txt"
artifact_source_digest_name="source-files.sha256"
artifact_result_digest_name="result.sha256"
artifact_stdout_digest_name="stdout.sha256"
artifact_exit_status_name="exit-status.txt"

if ! command -v python3 >/dev/null 2>&1; then
  echo "manifest exporter: python3 is required to validate benchmark-manifest.json" >&2
  exit 3
fi

# jq normalizes duplicate object members to the last value it sees. Reject
# those ambiguous declarations first, before any jq normalization or runner
# setup can occur. The validator uses only Python's standard library and also
# rejects non-standard JSON constants accepted by Python's decoder by default.
if ! python3 - "$manifest" <<'PY'
import json
import sys


def reject_duplicate_members(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate object member: {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError(f"non-standard JSON constant: {value}")


try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        json.load(
            stream,
            object_pairs_hook=reject_duplicate_members,
            parse_constant=reject_constant,
        )
except (OSError, UnicodeDecodeError, ValueError) as exc:
    print(f"manifest contract: invalid JSON ({exc})", file=sys.stderr)
    raise SystemExit(1)
PY
then
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "manifest exporter: jq is required to validate benchmark-manifest.json" >&2
  exit 3
fi

if [[ ! -f "$manifest" ]] || ! jq -e . "$manifest" >/dev/null 2>&1; then
  echo "manifest contract: benchmark-manifest.json is missing or invalid JSON" >&2
  exit 2
fi

# Ask the executable for the values it supports before checking the manifest.
# This contract-only path does not open a database, require a DSN, seed a
# corpus, or start a container.
# Use the selected SDK for every Go invocation, including export and version
# metadata. The acceptance harness supplies a guard that delegates only the
# exact contract-export command to Bazel's registered SDK.
go_bin="${AUTHZ_GO_BIN:-go}"
if [[ "$go_bin" != /* ]]; then
  go_bin="$(command -v "$go_bin" || true)"
fi
if [[ -z "$go_bin" || ! -x "$go_bin" ]]; then
  echo "manifest exporter: selected Go SDK executable not found" >&2
  exit 3
fi
contract_file="$(mktemp)"
trap 'rm -f "$contract_file"' EXIT
if ! (cd "$script_dir" && "$go_bin" run ./cmd/authz-spike -print-contract) >"$contract_file"; then
  echo "manifest exporter: executable contract export failed" >&2
  exit 3
fi
if ! jq -e . "$contract_file" >/dev/null 2>&1; then
  echo "manifest exporter: executable contract export returned invalid JSON" >&2
  exit 3
fi

contract_iterations="$(jq -er '.matrix.measured_iterations | select(type == "number" and . >= 1 and floor == .)' "$contract_file")" || {
  echo "manifest exporter: executable has no positive integer measured iteration count" >&2
  exit 3
}
contract_warmups="$(jq -er '.matrix.warmups | select(type == "number" and . >= 0 and floor == .)' "$contract_file")" || {
  echo "manifest exporter: executable has no non-negative integer warmup count" >&2
  exit 3
}

expected_top_level='["artifacts","commands","dataset","matrix","module","postgres_provisioning","schema","targets_ms"]'
manifest_top_level="$(jq -c '[keys[]] | sort' "$manifest")"
if [[ "$manifest_top_level" != "$expected_top_level" ]]; then
  echo "manifest contract: unsupported top-level declaration" >&2
  exit 2
fi

# Compare parsed JSON values.  jq ignores object member order, treats
# numerically equivalent spellings (20 and 20.0) as equal, and keeps array
# order meaningful.
comparison_status=0
jq -e --slurpfile executable "$contract_file" '
  {schema,module,matrix,dataset,targets_ms}
  == ($executable[0] | {schema,module,matrix,dataset,targets_ms})
' "$manifest" >/dev/null || comparison_status=$?
if [[ "$comparison_status" == 1 ]]; then
  echo "manifest contract: matrix, dataset, targets, schema, or module diverges from executable" >&2
  exit 2
fi
if [[ "$comparison_status" != 0 ]]; then
  echo "manifest exporter: parsed contract comparison failed" >&2
  exit 3
fi

runner_matrix_engines='["sqlite","postgres"]'
executable_matrix_engines="$(jq -c '.matrix.engines' "$contract_file")"
if [[ "$executable_matrix_engines" != "$runner_matrix_engines" ]]; then
  echo "manifest contract: executable engine set is unsupported by this runner" >&2
  exit 2
fi

expected_commands="$(jq -cn --arg iterations "$contract_iterations" '{
  sqlite: ("cd spikes/authz && AUTHZ_ENGINE=sqlite AUTHZ_ITERATIONS=" + $iterations + " AUTHZ_ARTIFACT_DIR=artifacts/sqlite ./run-benchmark.sh"),
  postgres: ("cd spikes/authz && AUTHZ_POSTGRES_DSN=<environment-only> AUTHZ_ENGINE=postgres AUTHZ_ITERATIONS=" + $iterations + " AUTHZ_ARTIFACT_DIR=artifacts/postgres ./run-benchmark.sh"),
  all: ("cd spikes/authz && AUTHZ_POSTGRES_DSN=<environment-only> AUTHZ_ENGINE=all AUTHZ_ITERATIONS=" + $iterations + " AUTHZ_ARTIFACT_DIR=artifacts/all ./run-benchmark.sh")
}')"
if ! jq -e --argjson expected "$expected_commands" '.commands == $expected' "$manifest" >/dev/null; then
  echo "manifest contract: command declarations diverge from executable runner" >&2
  exit 2
fi

expected_provisioning="$(jq -cn \
  --arg image 'postgres:16' \
  --arg container 'plug-swarm-authz' \
  --arg start 'docker run --rm --name plug-swarm-authz -e POSTGRES_PASSWORD="$AUTHZ_POSTGRES_PASSWORD" -e POSTGRES_DB=authz -p 127.0.0.1:55432:5432 postgres:16' \
  --arg dsn 'export AUTHZ_POSTGRES_DSN="postgres://postgres:${AUTHZ_POSTGRES_PASSWORD}@127.0.0.1:55432/authz?sslmode=disable"' \
  --arg capture 'AUTHZ_POSTGRES_PASSWORD and AUTHZ_POSTGRES_DSN are environment-only and are never written to artifacts' \
  '{image:$image,container_name:$container,start_command:$start,dsn_setup:$dsn,secret_capture:$capture}')"
if ! jq -e --argjson expected "$expected_provisioning" '.postgres_provisioning == $expected' "$manifest" >/dev/null; then
  echo "manifest contract: Postgres provisioning declaration diverges from executable runner" >&2
  exit 2
fi

expected_artifacts="$(jq -cn \
  --arg result "$artifact_result_name" \
  --arg stdout "$artifact_stdout_name" \
  --arg command "$artifact_command_name" \
  --arg environment "$artifact_environment_name" \
  --arg source "$artifact_source_digest_name" \
  --arg result_digest "$artifact_result_digest_name" \
  --arg stdout_digest "$artifact_stdout_digest_name" \
  --arg exit_status "$artifact_exit_status_name" \
  '{result:$result,stdout:$stdout,command:$command,environment:$environment,source_digest:$source,result_digest:$result_digest,stdout_digest:$stdout_digest,exit_status:$exit_status}')"
if ! jq -e --argjson expected "$expected_artifacts" '.artifacts == $expected' "$manifest" >/dev/null; then
  echo "manifest contract: artifact declarations diverge from executable runner" >&2
  exit 2
fi

manifest_iterations="$contract_iterations"
manifest_warmups="$contract_warmups"

engine="${AUTHZ_ENGINE:-all}"
iterations="${AUTHZ_ITERATIONS:-$manifest_iterations}"
artifact_dir="${AUTHZ_ARTIFACT_DIR:-$script_dir/artifacts/run}"
case "$artifact_dir" in
  /*) ;;
  *) artifact_dir="$(pwd -P)/$artifact_dir" ;;
esac

case "$engine" in
  sqlite|postgres|all) ;;
  *)
    echo "AUTHZ_ENGINE must be sqlite, postgres, or all (got $engine)" >&2
    exit 2
    ;;
esac
if [[ ! "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  echo "AUTHZ_ITERATIONS must be a positive integer (got $iterations)" >&2
  exit 2
fi
if [[ "$iterations" != "$manifest_iterations" ]]; then
  echo "AUTHZ_ITERATIONS must match benchmark-manifest.json matrix.measured_iterations ($manifest_iterations; got $iterations)" >&2
  exit 2
fi
if [[ "$engine" != sqlite && -z "${AUTHZ_POSTGRES_DSN:-}" ]]; then
  echo "AUTHZ_POSTGRES_DSN is required for postgres and all runs" >&2
  exit 2
fi

mkdir -p "$artifact_dir"
result="$artifact_dir/$artifact_result_name"
stdout="$artifact_dir/$artifact_stdout_name"
command_file="$artifact_dir/$artifact_command_name"
environment="$artifact_dir/$artifact_environment_name"
status_file="$artifact_dir/$artifact_exit_status_name"

case "$engine" in
  sqlite)
    display_command="go run ./cmd/authz-spike -engine=sqlite -iterations=$iterations -output=$result"
    ;;
  postgres)
    display_command="AUTHZ_POSTGRES_DSN=<environment-only> go run ./cmd/authz-spike -engine=postgres -iterations=$iterations -output=$result"
    ;;
  all)
    display_command="AUTHZ_POSTGRES_DSN=<environment-only> go run ./cmd/authz-spike -engine=all -iterations=$iterations -output=$result"
    ;;
esac
printf '%s\n' "cd spikes/authz && $display_command" > "$command_file"

{
  printf 'manifest=%s\n' "$(basename -- "$manifest")"
  printf 'manifest_sha256=%s\n' "$(sha256sum "$manifest" | awk '{print $1}')"
  printf 'engine=%s\n' "$engine"
  printf 'iterations=%s\n' "$iterations"
  printf 'warmups=%s\n' "$manifest_warmups"
  printf 'git_head=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
  printf 'git_worktree_status=%s\n' "$(git -C "$repo_root" status --porcelain=v1 | tr '\n' ' ')"
  printf 'go_version=%s\n' "$("$go_bin" version)"
  printf 'module=%s\n' "$(sed -n '1p' "$script_dir/go.mod")"
  printf 'postgres_dsn=captured-from-environment-only\n'
} > "$environment"

run_status=0
(
  cd "$script_dir"
  if [[ "$engine" == sqlite ]]; then
    "$go_bin" run ./cmd/authz-spike -engine=sqlite -iterations="$iterations" -output="$result"
  else
    # The binary already reads AUTHZ_POSTGRES_DSN as its default.  Assigning
    # it on this command keeps the value out of the parent shell and logs.
    AUTHZ_POSTGRES_DSN="$AUTHZ_POSTGRES_DSN" \
      "$go_bin" run ./cmd/authz-spike -engine="$engine" -iterations="$iterations" -output="$result"
  fi
) > "$stdout" 2>&1 || run_status=$?
printf '%s\n' "$run_status" > "$status_file"

if [[ "$run_status" -ne 0 ]]; then
  echo "authorization benchmark failed (exit $run_status); see $stdout" >&2
  exit "$run_status"
fi

# Hash tracked source files in lexical path order.  This excludes the output
# directory and records the exact source inputs without embedding any secret.
(
  cd "$repo_root"
  # Hash one path per invocation.  The pinned Bazelisk sha256sum available on
  # agent hosts accepts one file argument; a single xargs call would silently
  # produce an empty digest when it rejects a multi-file invocation.
  git ls-files -z spikes/authz | sort -z |
    while IFS= read -r -d '' path; do
      sha256sum "$path"
    done
) > "$artifact_dir/$artifact_source_digest_name"
sha256sum "$result" > "$artifact_dir/$artifact_result_digest_name"
sha256sum "$stdout" > "$artifact_dir/$artifact_stdout_digest_name"

echo "authorization benchmark complete: $artifact_dir"
