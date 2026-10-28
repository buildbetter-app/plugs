#!/usr/bin/env bash

# Run the authorization spike with the checked-in matrix and retain enough
# metadata to rerun and audit the exact invocation.  The Postgres DSN is read
# only from AUTHZ_POSTGRES_DSN and is never copied into an artifact.

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
repo_root="$(CDPATH= cd -- "$script_dir/../.." && pwd -P)"
manifest="$script_dir/benchmark-manifest.json"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to validate benchmark-manifest.json" >&2
  exit 2
fi
manifest_iterations="$(jq -er '.matrix.measured_iterations | select(type == "number" and . >= 1 and floor == .)' "$manifest")" || {
  echo "benchmark manifest has no positive integer matrix.measured_iterations" >&2
  exit 2
}
manifest_warmups="$(jq -er '.matrix.warmups | select(type == "number" and . >= 0 and floor == .)' "$manifest")" || {
  echo "benchmark manifest has no non-negative integer matrix.warmups" >&2
  exit 2
}
if [[ "$manifest_warmups" != 3 ]]; then
  echo "authz benchmark supports exactly 3 warmups (manifest says $manifest_warmups)" >&2
  exit 2
fi

engine="${AUTHZ_ENGINE:-all}"
iterations="${AUTHZ_ITERATIONS:-20}"
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
result="$artifact_dir/result.json"
stdout="$artifact_dir/stdout.log"
command_file="$artifact_dir/command.txt"
environment="$artifact_dir/environment.txt"
status_file="$artifact_dir/exit-status.txt"

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
  printf 'go_version=%s\n' "$(go version)"
  printf 'module=%s\n' "$(sed -n '1p' "$script_dir/go.mod")"
  printf 'postgres_dsn=captured-from-environment-only\n'
} > "$environment"

run_status=0
(
  cd "$script_dir"
  if [[ "$engine" == sqlite ]]; then
    go run ./cmd/authz-spike -engine=sqlite -iterations="$iterations" -output="$result"
  else
    # The binary already reads AUTHZ_POSTGRES_DSN as its default.  Assigning
    # it on this command keeps the value out of the parent shell and logs.
    AUTHZ_POSTGRES_DSN="$AUTHZ_POSTGRES_DSN" \
      go run ./cmd/authz-spike -engine="$engine" -iterations="$iterations" -output="$result"
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
  git ls-files -z spikes/authz | sort -z | xargs -0 sha256sum
) > "$artifact_dir/source-files.sha256"
sha256sum "$result" > "$artifact_dir/result.sha256"
sha256sum "$stdout" > "$artifact_dir/stdout.sha256"

echo "authorization benchmark complete: $artifact_dir"
