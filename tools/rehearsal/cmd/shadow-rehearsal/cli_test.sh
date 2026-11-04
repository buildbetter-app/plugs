#!/usr/bin/env bash
set -euo pipefail

binary="$TEST_SRCDIR/$TEST_WORKSPACE/$1"
source_root="${PLUG_SOURCE_MODULE_ROOT:?exact RunsOn source checkout required}"
source_sha="$(git -C "$source_root" rev-parse HEAD)"
source_tree="$(git -C "$source_root" rev-parse 'HEAD^{tree}')"

[[ "$("$binary")" == PLAN:* ]]
if R1_OLD_RUNTIME_BINARY=missing "$binary" --local --driver memory --source-sha "$source_sha" --source-tree "$source_tree" >/dev/null 2>&1; then
  printf '%s\n' 'conflicting edition output was accepted' >&2
  exit 1
fi

env -u TEST_SRCDIR -u TEST_WORKSPACE -u PLUG_SOURCE_MODULE_ROOT \
  RUNFILES_DIR="$TEST_SRCDIR" BUILD_WORKSPACE_DIRECTORY="$source_root" \
  "$binary" --local --driver memory --source-sha "$source_sha" --source-tree "$source_tree" >"$TEST_TMPDIR/disposable-report.json"
python3 - "$TEST_TMPDIR/disposable-report.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    report = json.load(source)
assert report["preparation"] == "PASS"
assert report["resources_cleaned"] is True
assert report["r1_qualification"] == "WAIT"
assert len(report["runs"]) == 3
assert all(run["public_transcript_compared"] and run["public_transcript_exchanges"] == 3 for run in report["runs"])
rollback = report["runtime_rollback"]
assert rollback["complete"] == "PASS_executable_image_prompt_plugin_data_transition"
assert rollback["cleanup_verified"] is True
assert rollback["wrong_digest_refused"] is True
assert rollback["pin_mismatch_refused"] is True
assert len(rollback["runtime_observations"]) == 3
PY
printf '%s\n' 'I8_DISPOSABLE_CLI_PASS actual_public_transcript_and_complete_owned_rollback'
