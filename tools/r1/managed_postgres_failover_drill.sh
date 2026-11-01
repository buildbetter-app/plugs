#!/usr/bin/env bash
# R1 preparation harness: plan or run the disposable Lane-A failover fixture.
# This script never targets a managed, staging, production, or customer DSN.
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_VERSION="r1-managed-postgres-failover-v1"
readonly POSTGRES_IMAGE="postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
readonly BASE_WORK_ROOT="/tmp/plug-swarm/work"
WORK_ROOT="${R1_WORK_ROOT:-$BASE_WORK_ROOT}"
readonly REMOTE_BAZEL="/tmp/plug-swarm/bin/remote-bazel"
readonly FAILOVER_FILTER='^(TestPublicHTTPClaimNextTwoHundredAgentsPostgres(Failover|LateFailover))$'

mode=plan
commit=""
label=""
output_dir=""
agents=200
kill_after=100
network=""
primary=""
standby=""

usage() {
  cat <<USAGE
Usage: $0 [--plan | --local-preflight | --runs-on COMMIT] [options]

Safe defaults:
  --plan              validate the acceptance envelope and print commands (default)
  --local-preflight   start two disposable pinned PostgreSQL containers, probe readiness,
                      and remove them; this is topology smoke only
  --runs-on COMMIT    run the existing exact-head failover tests through remote-bazel

Options:
  --label LABEL       RunsOn label (letters, digits, dot, underscore, hyphen)
  --output DIR        Evidence output directory under /tmp/plug-swarm/work/
  --agents N          Fixed fixture load; only 200 is accepted
  --kill-after N      Fixed late-kill threshold; only 100 is accepted
  --help              Show this help

The RunsOn mode executes TestPublicHTTPClaimNextTwoHundredAgentsPostgresFailover and
TestPublicHTTPClaimNextTwoHundredAgentsPostgresLateFailover from the supplied commit.
Its receipt, not this wrapper, is the qualification artifact. Managed HA, production,
RPO/RTO, and rollback remain unqualified by this prep harness.
USAGE
}

die() {
  echo "R1-FAIL: $*" >&2
  exit 2
}

note() {
  printf 'R1: %s\n' "$*"
}

safe_work_path() {
  local path=$1
  path=$(realpath -m -- "$path")
  case "$path" in
    "$WORK_ROOT"|"$WORK_ROOT"/*) ;;
    *) die "path must remain under $WORK_ROOT: $path" ;;
  esac
}

validate_envelope() {
  [[ "$agents" == 200 && "$kill_after" == 100 ]] || die "the existing fixture fixes agents=200 and late kill-after=100"
  [[ -z "${R1_PRIMARY_DSN:-}" && -z "${R1_STANDBY_DSN:-}" ]] || die "external DSNs are forbidden; use the owned fixture"
  [[ -z "${PGHOST:-}" && -z "${PGSERVICE:-}" ]] || die "ambient PostgreSQL routing is forbidden"
  [[ "$POSTGRES_IMAGE" == postgres@sha256:* ]] || die "PostgreSQL image must remain digest pinned"
  [[ $(realpath -m -- "$BASE_WORK_ROOT") == "$BASE_WORK_ROOT" ]] || die "base work root must not resolve elsewhere"
  WORK_ROOT=$(realpath -m -- "$WORK_ROOT")
  case "$WORK_ROOT" in
    "$BASE_WORK_ROOT"|"$BASE_WORK_ROOT"/*) ;;
    *) die "R1_WORK_ROOT must remain under $BASE_WORK_ROOT" ;;
  esac
  mkdir -p -- "$WORK_ROOT"
}

make_output_dir() {
  if [[ -z "$output_dir" ]]; then
    output_dir=$(mktemp -d "$WORK_ROOT/r1-failover.XXXXXX")
  else
    safe_work_path "$output_dir"
    output_dir=$(realpath -m -- "$output_dir")
    mkdir -- "$output_dir" || die "output directory must be new"
  fi
  chmod 700 "$output_dir"
}

write_plan() {
  make_output_dir
  cat > "$output_dir/plan.txt" <<PLAN
script=$SCRIPT_VERSION
mode=$mode
postgres_image=$POSTGRES_IMAGE
agents=$agents
late_kill_after_acknowledged=$kill_after
early_kill_after_acknowledged=1
work_root=$WORK_ROOT
output_dir=$output_dir
fixture=internal/integration/e5acceptance:local_wasi_test
filter=$FAILOVER_FILTER
local_topology=two disposable containers on a private Docker network; no host port publication
required_metrics=primary_acknowledged_at_kill,distinct_winners,acknowledged_missing_after_promotion,old_writer_fencing,kill_to_first_new_primary_commit_seconds
managed_ha=UNQUALIFIED
production=UNQUALIFIED
rollback=UNQUALIFIED
rpo_rto=UNQUALIFIED
rpo_target_seconds=300 (design target, not a measured result)
rto_target_seconds=3600 (design target, not a measured result)
managed_drill=WAIT_SEPARATE_AUTHORIZATION
managed_drill_steps=record approved disposable topology; seed authenticated claims; capture acknowledgments; invoke approved provider failover after >=100 acknowledgments; reconnect; verify distinct winners, zero acknowledged loss, lease fencing and new incarnation; measure application recovery and compare target RPO/RTO; restore disposable topology
provider_action=NOT_IMPLEMENTED (no cloud API or managed DSN accepted)
PLAN
  note "plan written to $output_dir/plan.txt"
  note "no container, network, external DSN, staging, production, or customer data was touched"
  note "A1 remains bounded to the controlled two-container streaming-replication/WASI fixture"
}

cleanup_local() {
  local status=$?
  trap - EXIT INT TERM
  local cleanup_status=0
  if [[ -n "$primary" ]]; then docker_cmd rm -f "$primary" >/dev/null 2>&1 || cleanup_status=1; fi
  if [[ -n "$standby" ]]; then docker_cmd rm -f "$standby" >/dev/null 2>&1 || cleanup_status=1; fi
  if [[ -n "$network" ]]; then docker_cmd network rm "$network" >/dev/null 2>&1 || cleanup_status=1; fi
  if (( cleanup_status != 0 )); then
    note "cleanup failed; inspect the owned resource IDs in $output_dir/local-resources.txt"
    status=1
  elif [[ -f "$output_dir/local-preflight.txt" ]]; then
    echo "cleanup=PASS" >> "$output_dir/local-preflight.txt"
    note "owned containers and network removed"
  fi
  exit "$status"
}

# Pin the local Unix socket; ambient Docker contexts cannot redirect this drill.
docker_cmd() {
  docker --host=unix:///var/run/docker.sock "$@"
}

wait_ready() {
  local container=$1
  local attempt
  for ((attempt=1; attempt<=60; attempt++)); do
    if docker_cmd exec "$container" pg_isready -U postgres -d plug_r1 >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

run_local_preflight() {
  command -v docker >/dev/null 2>&1 || die "docker is required for --local-preflight"
  docker_cmd info >/dev/null 2>&1 || die "Docker socket is unavailable"
  make_output_dir
  local resource_name="plug-swarm-r1-$RANDOM-$$"
  trap cleanup_local EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  network=$(docker_cmd network create --internal "$resource_name")
  echo "network=$network" > "$output_dir/local-resources.txt"
  primary=$(docker_cmd run -d --name "$resource_name-primary" --network "$network" \
    -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=plug_r1 "$POSTGRES_IMAGE")
  echo "primary=$primary" >> "$output_dir/local-resources.txt"
  standby=$(docker_cmd run -d --name "$resource_name-standby" --network "$network" \
    -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=plug_r1 "$POSTGRES_IMAGE")
  echo "standby=$standby" >> "$output_dir/local-resources.txt"
  wait_ready "$primary" || die "primary readiness probe failed"
  wait_ready "$standby" || die "standby readiness probe failed"
  {
    echo "script=$SCRIPT_VERSION"
    echo "mode=local-preflight"
    echo "postgres_image=$POSTGRES_IMAGE"
    echo "network=$network"
    echo "primary=$primary"
    echo "standby=$standby"
    echo "readiness=PASS"
    echo "replication=UNQUALIFIED (preflight does not promote a managed standby)"
    echo "claim_workload=UNQUALIFIED (use --runs-on for the exact 200-agent test)"
    echo "managed_ha=UNQUALIFIED"
    echo "production=UNQUALIFIED"
    echo "rollback=UNQUALIFIED"
  } > "$output_dir/local-preflight.txt"
  note "two pinned disposable containers reached readiness; report $output_dir/local-preflight.txt"
  note "no replication promotion or Plug workload was claimed by this smoke"
}

run_runson() {
  [[ -x "$REMOTE_BAZEL" ]] || die "remote-bazel wrapper is unavailable: $REMOTE_BAZEL"
  [[ "$commit" =~ ^[0-9a-fA-F]{40}$ ]] || die "--runs-on requires a full 40-hex commit"
  git cat-file -e "$commit^{commit}" 2>/dev/null || die "commit is not present locally: $commit"
  if [[ -z "$label" ]]; then
    label="r1-failover-${commit:0:12}-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  [[ "$label" =~ ^[A-Za-z0-9._-]{1,67}$ ]] || die "invalid RunsOn label (maximum 67 characters): $label"
  make_output_dir
  local repo command_file log status
  repo=$(git rev-parse --show-toplevel)
  command_file="$output_dir/remote-bazel-command.txt"
  log="$output_dir/remote-bazel.log"
  printf '%q ' "$REMOTE_BAZEL" "$repo" "$commit" "$label" -- test \
    //internal/integration/e5acceptance:local_wasi_test \
    "--test_filter=$FAILOVER_FILTER" --test_output=all --cache_test_results=no \
    --test_timeout=1800 --test_arg=-test.v --test_env=PLUG_E5_FAILOVER=1 > "$command_file"
  note "dispatching exact-head failover gate through remote-bazel; output is $output_dir"
  set +e
  "$REMOTE_BAZEL" "$repo" "$commit" "$label" -- test \
    //internal/integration/e5acceptance:local_wasi_test \
    "--test_filter=$FAILOVER_FILTER" --test_output=all --cache_test_results=no \
    --test_timeout=1800 --test_arg=-test.v --test_env=PLUG_E5_FAILOVER=1 2>&1 | tee "$log"
  status=${PIPESTATUS[0]}
  set -e
  echo "$status" > "$output_dir/remote-bazel-exit-code.txt"
  if (( status != 0 )); then
    die "remote-bazel failed with exit $status; inspect $log"
  fi
  note "remote-bazel exited zero; qualification requires the exact receipt, both non-skipped cases and their metrics in $log"
  note "managed HA, production, and rollback remain UNQUALIFIED"
}

while (($#)); do
  case "$1" in
    --plan) mode=plan; shift ;;
    --local-preflight) mode=local-preflight; shift ;;
    --runs-on) mode=runson; [[ $# -ge 2 ]] || die "--runs-on needs COMMIT"; commit=$2; shift 2 ;;
    --label) [[ $# -ge 2 ]] || die "--label needs LABEL"; label=$2; shift 2 ;;
    --output) [[ $# -ge 2 ]] || die "--output needs DIR"; output_dir=$2; shift 2 ;;
    --agents) [[ $# -ge 2 ]] || die "--agents needs N"; agents=$2; shift 2 ;;
    --kill-after) [[ $# -ge 2 ]] || die "--kill-after needs N"; kill_after=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

validate_envelope
case "$mode" in
  plan) write_plan ;;
  local-preflight) run_local_preflight ;;
  runson) run_runson ;;
  *) die "unknown mode: $mode" ;;
esac
