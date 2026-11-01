#!/usr/bin/env bash
# R1 preparation harness for a disposable software/data rollback rehearsal.
# It never opens, writes, or uploads a production/customer store.
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_VERSION="r1-rollback-rehearsal-v1"
readonly BASE_WORK_ROOT="/tmp/plug-swarm/work"
WORK_ROOT="${R1_WORK_ROOT:-$BASE_WORK_ROOT}"

mode=plan
source_dir=""
plug_head_dump=""
pre_manifest=""
post_manifest=""
reverse_command=""
output_dir=""

usage() {
  cat <<USAGE
Usage: $0 [--plan | --rehearse] [options]

Safe defaults:
  --plan                  print the rollback contract (default)
  --rehearse              copy and freeze only disposable inputs under /tmp/plug-swarm/work/

Rehearsal options:
  --source-dir DIR        disposable Beads copy; must be under /tmp/plug-swarm/work/
  --plug-head-dump FILE   synthetic or disposable Plug head-state dump
  --pre-switch-manifest FILE  pinned software/prompt/plugin manifest before switch
  --post-switch-manifest FILE pinned software/prompt/plugin manifest after switch
  --reverse-command FILE  optional executable test adapter: FILE INPUT_DUMP OUTPUT_DIR
  --output DIR             evidence directory under /tmp/plug-swarm/work/
  --help                   show this help

The harness freezes the copied Beads input read-only, verifies source immutability,
and records digest-bound inputs. Adapter execution alone never qualifies reverse
conversion or loss accounting. Production, staging, customer data,
managed HA, deployment and release are never touched.
USAGE
}

die() {
  echo "R1-ROLLBACK-FAIL: $*" >&2
  exit 2
}

note() {
  printf 'R1-ROLLBACK: %s\n' "$*"
}

assert_work_root() {
  [[ $(realpath -m -- "$BASE_WORK_ROOT") == "$BASE_WORK_ROOT" ]] || die "base work root must not resolve elsewhere"
  WORK_ROOT=$(realpath -m -- "$WORK_ROOT")
  case "$WORK_ROOT" in
    "$BASE_WORK_ROOT"|"$BASE_WORK_ROOT"/*) ;;
    *) die "R1_WORK_ROOT must remain under $BASE_WORK_ROOT" ;;
  esac
  mkdir -p "$WORK_ROOT"
}

safe_path() {
  local path=$1
  path=$(realpath -m -- "$path")
  case "$path" in
    "$WORK_ROOT"|"$WORK_ROOT"/*) ;;
    *) die "path must remain under $WORK_ROOT: $path" ;;
  esac
}

file_digest() {
  sha256sum "$1" | awk '{print $1}'
}

tree_digest() {
  local dir=$1
  tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -cf - -C "$dir" . | python3 -c '
import hashlib
import sys
digest = hashlib.sha256()
for chunk in iter(lambda: sys.stdin.buffer.read(1048576), b""):
    digest.update(chunk)
print(digest.hexdigest())'
}

make_output_dir() {
  if [[ -z "$output_dir" ]]; then
    output_dir=$(mktemp -d "$WORK_ROOT/r1-rollback.XXXXXX")
  else
    safe_path "$output_dir"
    output_dir=$(realpath -m -- "$output_dir")
    mkdir -- "$output_dir" || die "output directory must be new"
  fi
  chmod 700 "$output_dir"
}

write_plan() {
  make_output_dir
  cat > "$output_dir/plan.txt" <<PLAN
script=$SCRIPT_VERSION
mode=plan
software_rollback_plan=pin pre-switch image/prompt/plugin manifests by digest; rehearse redeployment only in disposable resources
data_rollback=keep Beads read-only; reverse a Plug head-state dump into a disposable frozen Beads copy
source_mutation=forbidden
reverse_conversion=UNQUALIFIED (an adapter exit alone does not qualify fidelity)
software_rollback=UNQUALIFIED (manifests are recorded; no redeployment is performed)
loss_accounting=UNQUALIFIED until a real reverse report lists history, Plug-only kinds, blobs, principals, warnings, and unsupported fields
managed_ha=UNQUALIFIED
production=UNQUALIFIED
customer_data=forbidden
PLAN
  note "plan written to $output_dir/plan.txt"
  note "no source, store, cloud resource, deployment, or converter was touched"
}

run_reverse() {
  local input_dump=$1 reverse_output=$2
  [[ -x "$reverse_command" ]] || die "reverse command is not executable: $reverse_command"
  safe_path "$reverse_command"
  reverse_command=$(realpath -e -- "$reverse_command")
  mkdir -p "$reverse_output"
  # The adapter contract is intentionally tiny and disposable: input dump then output dir.
  # This is a trusted adapter, not a sandbox. Never pass cloud/store credentials.
  (cd "$reverse_output" && env -i PATH="$PATH" "$reverse_command" "$input_dump" "$reverse_output")
}

run_rehearsal() {
  [[ -d "$source_dir" ]] || die "source directory does not exist: $source_dir"
  [[ -f "$plug_head_dump" ]] || die "Plug head dump does not exist: $plug_head_dump"
  [[ -f "$pre_manifest" ]] || die "pre-switch manifest does not exist: $pre_manifest"
  [[ -f "$post_manifest" ]] || die "post-switch manifest does not exist: $post_manifest"
  safe_path "$source_dir"
  safe_path "$plug_head_dump"
  safe_path "$pre_manifest"
  safe_path "$post_manifest"
  source_dir=$(realpath -e -- "$source_dir")
  [[ "$source_dir" != "$WORK_ROOT" ]] || die "source must be a dedicated directory below the work root"
  if [[ -n "$output_dir" ]]; then
    case "$(realpath -m -- "$output_dir")/" in
      "$source_dir/"*) die "output must not be inside the source tree" ;;
    esac
  fi
  python3 - "$source_dir" <<'PY'
import os, stat, sys
for directory, dirs, files in os.walk(sys.argv[1], followlinks=False):
    for name in dirs + files:
        mode = os.lstat(os.path.join(directory, name)).st_mode
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            sys.exit("R1-ROLLBACK-FAIL: source must contain only regular files and directories")
PY
  make_output_dir

  local before after frozen dump_copy reverse_output reverse_status
  before=$(tree_digest "$source_dir")
  frozen="$output_dir/beads-readonly"
  cp -a --reflink=auto "$source_dir" "$frozen"
  chmod -R a-w "$frozen"
  python3 - "$frozen" <<'PY'
import os, sys
for directory, dirs, files in os.walk(sys.argv[1]):
    for path in [directory] + [os.path.join(directory, name) for name in files]:
        if os.stat(path).st_mode & 0o222:
            sys.exit("R1-ROLLBACK-FAIL: frozen copy has write permission bits")
PY

  dump_copy="$output_dir/plug-head.dump"
  cp -- "$plug_head_dump" "$dump_copy"
  chmod 400 "$dump_copy"
  cp -- "$pre_manifest" "$output_dir/pre-switch-manifest"
  cp -- "$post_manifest" "$output_dir/post-switch-manifest"
  chmod 400 "$output_dir/pre-switch-manifest" "$output_dir/post-switch-manifest"
  reverse_status=NOT_RUN
  reverse_output="$output_dir/reverse-output"
  if [[ -n "$reverse_command" ]]; then
    run_reverse "$dump_copy" "$reverse_output"
    reverse_status=PASS
  fi
  after=$(tree_digest "$source_dir")
  [[ "$before" == "$after" ]] || die "source changed during rollback preparation"

  {
    echo "script=$SCRIPT_VERSION"
    echo "mode=rehearse"
    echo "source_dir=$source_dir"
    echo "source_digest_before=$before"
    echo "source_digest_after=$after"
    echo "source_unchanged=$([[ "$before" == "$after" ]] && echo PASS || echo FAIL)"
    echo "beads_copy=$frozen"
    echo "beads_copy_readonly=PASS"
    echo "plug_head_dump_digest=$(file_digest "$dump_copy")"
    echo "pre_switch_manifest_digest=$(file_digest "$output_dir/pre-switch-manifest")"
    echo "post_switch_manifest_digest=$(file_digest "$output_dir/post-switch-manifest")"
    echo "reverse_adapter_execution=$reverse_status"
    echo "reverse_conversion=UNQUALIFIED"
    echo "loss_accounting=UNQUALIFIED"
    echo "software_rollback=UNQUALIFIED (no redeployment performed)"
    echo "managed_ha=UNQUALIFIED"
    echo "production=UNQUALIFIED"
    echo "customer_data=NOT_USED"
  } > "$output_dir/rollback-rehearsal.txt"
  note "disposable Beads copy frozen read-only; source digest stayed $before"
  note "rollback report: $output_dir/rollback-rehearsal.txt"
  if [[ "$reverse_status" == NOT_RUN ]]; then
    note "reverse conversion was not invoked; loss accounting remains UNQUALIFIED"
  else
    note "trusted reverse adapter completed in disposable output $reverse_output; fidelity remains UNQUALIFIED"
  fi
}

while (($#)); do
  case "$1" in
    --plan) mode=plan; shift ;;
    --rehearse) mode=rehearse; shift ;;
    --source-dir) [[ $# -ge 2 ]] || die "--source-dir needs DIR"; source_dir=$2; shift 2 ;;
    --plug-head-dump) [[ $# -ge 2 ]] || die "--plug-head-dump needs FILE"; plug_head_dump=$2; shift 2 ;;
    --pre-switch-manifest) [[ $# -ge 2 ]] || die "--pre-switch-manifest needs FILE"; pre_manifest=$2; shift 2 ;;
    --post-switch-manifest) [[ $# -ge 2 ]] || die "--post-switch-manifest needs FILE"; post_manifest=$2; shift 2 ;;
    --reverse-command) [[ $# -ge 2 ]] || die "--reverse-command needs FILE"; reverse_command=$2; shift 2 ;;
    --output) [[ $# -ge 2 ]] || die "--output needs DIR"; output_dir=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

assert_work_root
case "$mode" in
  plan) write_plan ;;
  rehearse) run_rehearsal ;;
  *) die "unknown mode: $mode" ;;
esac
