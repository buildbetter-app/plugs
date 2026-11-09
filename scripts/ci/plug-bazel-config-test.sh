#!/usr/bin/env bash
# Static and executable checks for the Plug CI contract.
set -euo pipefail

if [[ -n "${TEST_SRCDIR:-}" || -n "${TEST_WORKSPACE:-}" ]]; then
  [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" && -d "$TEST_SRCDIR/$TEST_WORKSPACE" ]] || {
    echo "config: Bazel runfiles workspace is missing" >&2
    exit 1
  }
  repo_root="$TEST_SRCDIR/$TEST_WORKSPACE"
else
  repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
cd "$repo_root"
workflow=.github/workflows/plug-bazel.yml
nightly_workflow=.github/workflows/plug-nightly-restore-diffs.yml
public_workflow=.github/workflows/plug-public-bundle.yml
remote_workflow=.github/workflows/plug-remote-bazel.yml
targets=scripts/ci/plug-bazel-targets.sh
target_tests=scripts/ci/plug-bazel-targets-test.sh
collector=scripts/ci/plug-bazel-delivery.py
delivery_tests=scripts/ci/plug-bazel-delivery-test.sh

[[ -f "$workflow" ]] || { echo "config: missing $workflow" >&2; exit 1; }
[[ -f "$nightly_workflow" ]] || { echo "config: missing $nightly_workflow" >&2; exit 1; }
[[ -f "$remote_workflow" ]] || { echo "config: missing $remote_workflow" >&2; exit 1; }
[[ -x "$targets" ]] || { echo "config: $targets is not executable" >&2; exit 1; }
[[ -x "$target_tests" ]] || { echo "config: $target_tests is not executable" >&2; exit 1; }
[[ -x "$collector" ]] || { echo "config: $collector is not executable" >&2; exit 1; }
[[ -x "$delivery_tests" ]] || { echo "config: $delivery_tests is not executable" >&2; exit 1; }
bash -n "$targets"
bash -n "$target_tests"
bash -n "$delivery_tests"

python3 - "$workflow" "$targets" "$collector" "$remote_workflow" "$nightly_workflow" "$public_workflow" <<'PYTHON'
from pathlib import Path
import ast
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
remote_workflow = Path(sys.argv[4]).read_text(encoding="utf-8")
nightly_workflow = Path(sys.argv[5]).read_text(encoding="utf-8")
public_path = Path(sys.argv[6])
public_workflow = public_path.read_text(encoding="utf-8") if public_path.exists() else None
targets = Path(sys.argv[2]).read_text(encoding="utf-8")
collector = Path(sys.argv[3]).read_text(encoding="utf-8")
ast.parse(collector)  # Syntax without writing a cache into the source tree.
# Existing executable flag/selection assertions follow their implementation
# into the collector. Comments alone cannot satisfy these checks.
workflow_code = "\n".join(line for line in workflow.splitlines() if not line.lstrip().startswith("#"))
collector_literals = {node.value for node in ast.walk(ast.parse(collector))
                      if isinstance(node, ast.Constant) and isinstance(node.value, str)}

required_workflows = {
    "plug-bazel.yml", "plug-nightly-restore-diffs.yml", "plug-remote-bazel.yml", "plug-runs-on-smoke.yml",
}
allowed_workflows = required_workflows | {"plug-public-bundle.yml", "plug-beads-bazel.yml", "plug-cp7-latency.yml"}

def check_workflow_inventory(directory):
    workflow_files = {
        path.name
        for pattern in ("*.yml", "*.yaml")
        for path in directory.glob(pattern)
    }
    missing = sorted(required_workflows - workflow_files)
    unexpected = sorted(workflow_files - allowed_workflows)
    if missing:
        raise SystemExit("config: required workflow files are missing: " + ", ".join(missing))
    if unexpected:
        raise SystemExit("config: inherited workflow files remain: " + ", ".join(unexpected))

# Exercise the actual inventory guard with disposable names, independently of
# PR260 source. CP7 is optional; similarly named additions remain refused.
with tempfile.TemporaryDirectory(prefix="plug-ci-workflow-inventory.") as temporary:
    fixture = Path(temporary)
    for name in required_workflows | {"plug-public-bundle.yml", "plug-beads-bazel.yml"}:
        (fixture / name).touch()
    check_workflow_inventory(fixture)
    cp7_workflow = fixture / "plug-cp7-latency.yml"
    cp7_workflow.touch()
    check_workflow_inventory(fixture)
    for name, message in (
        ("plug-cp7-latency-extra.yml", "config: inherited workflow files remain: plug-cp7-latency-extra.yml"),
        ("unexpected.yaml", "config: inherited workflow files remain: unexpected.yaml"),
    ):
        unknown = fixture / name
        unknown.touch()
        try:
            check_workflow_inventory(fixture)
        except SystemExit as error:
            if str(error) != message:
                raise
        else:
            raise SystemExit("config: inventory accepted an unapproved workflow: " + name)
        unknown.unlink()
    cp7_workflow.unlink()
    check_workflow_inventory(fixture)
    (fixture / "plug-bazel.yml").unlink()
    try:
        check_workflow_inventory(fixture)
    except SystemExit as error:
        if str(error) != "config: required workflow files are missing: plug-bazel.yml":
            raise
    else:
        raise SystemExit("config: inventory accepted a missing required workflow")
print("plug-ci-config: CP7 optional workflow admission and inventory refusal fixtures passed")

# Preserve the old 45-minute job envelope even if the two new 150-minute
# latency targets run serially. Leave 15 minutes for finalization within the
# six-hour GitHub-hosted fallback limit; test-class budgets stay unchanged.
expected_timeout_map = "900,900,900,9000"
job_minutes = [int(value) for value in re.findall(r"(?m)^    timeout-minutes: (\d+)\s*$", workflow_code)]
workflow_timeout_maps = re.findall(r"(?m)^\s*test --test_timeout=(\S+)\s*$", workflow_code)
timeout_errors = []
if job_minutes != [360] or not 45 + 2 * 150 + 15 <= job_minutes[0] <= 360:
    timeout_errors.append("ordinary PR job needs the explicit 360-minute envelope (45 + 150 + 150 + 15)")
if workflow_timeout_maps != [expected_timeout_map]:
    timeout_errors.append("workflow must use only --test_timeout=" + expected_timeout_map)
collector_timeout_maps = {value for value in collector_literals if value.startswith("--test_timeout=")}
if collector_timeout_maps != {"--test_timeout=" + expected_timeout_map}:
    timeout_errors.append("delivery must preserve 900s for short/moderate/long and allow 9000s only for eternal")
if timeout_errors:
    raise SystemExit("config: " + "; ".join(timeout_errors))

required_workflow = {
    "name: Plug Bazel",
    "pull_request:",
    "workflow_dispatch:",
    "format('runs-on={0}/runner=bb-x64-16', github.run_id)",
    "runs-on/action@",
    "RUNSON_TRUSTED",
    "actions/setup-node@",
    "node-version: 24",
    "command -v google-chrome",
    "permissions:\n  contents: read",
    "actions/cache/restore@",
    "actions/upload-artifact@",
    "PLUG_BAZEL_EVIDENCE_DIR=$RUNNER_TEMP/plug-bazel-evidence",
    "PLUG_BAZEL_TARGETS_DIR=$RUNNER_TEMP/plug-bazel-targets",
    "PLUG_BAZEL_REPOSITORY_CACHE=$RUNNER_TEMP/plug-bazel-repository-cache",
    'export PLUG_BAZEL_EVIDENCE_DIR="$RUNNER_TEMP/plug-bazel-evidence"',
    'export PLUG_BAZEL_TARGETS_DIR="$RUNNER_TEMP/plug-bazel-targets"',
    'export PLUG_BAZEL_REPOSITORY_CACHE="$RUNNER_TEMP/plug-bazel-repository-cache"',
    '>> "$GITHUB_ENV"',
}
missing = sorted(fragment for fragment in required_workflow if fragment not in workflow_code)
if missing:
    raise SystemExit("config: workflow is missing: " + ", ".join(missing))
required_collector = {
    "//:lexicon_gate", "touched-targets.txt", "reverse-targets.txt",
    "--lockfile_mode=error", "--nocache_test_results", "--flaky_test_attempts=1", "--test_timeout=900,900,900,9000",
    "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", "--remote_cache=", "--remote_executor=",
    "--repo_contents_cache=", "--jobs=2", "--local_resources=memory=8192", "--test_env=GO_TEST_WRAP_TESTV=1",
}
if missing := sorted(required_collector - collector_literals):
    raise SystemExit("config: collector is missing executable literals: " + ", ".join(missing))
# One complete-selection predicate drives workflow preparation and execution.
public_targets = next(node.value for node in ast.parse(collector).body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == "PUBLIC_RUNTIME_TARGETS" for target in node.targets))
if (not isinstance(public_targets, ast.Call) or not isinstance(public_targets.func, ast.Name)
        or public_targets.func.id != "frozenset" or len(public_targets.args) != 1
        or ast.literal_eval(public_targets.args[0]) != {"//tools/roundtrip:public_beads_runtime_test",
                                                       "//spec/s1-b25-b28:public_recording_test"}):
    raise SystemExit("config: both mandatory native consumer labels must share selection")
selection_step = workflow_code.split("- name: Resolve complete Plug Bazel selection", 1)[1].split("- name:", 1)[0]
if ('plug-bazel-delivery.py selection --output' not in selection_step
        or 'plug-bazel-delivery.py public-selection --output' not in selection_step
        or selection_step.index('plug-bazel-delivery.py selection --output') > selection_step.index('plug-bazel-delivery.py public-selection --output')):
    raise SystemExit("config: complete source-bound selection must precede public preparation")
if "GH_TOKEN" in selection_step:
    raise SystemExit("config: selection cannot receive the artifact-read token")

# Both privileged paths fetch and hash the same independently admitted helper.
helper_bindings = []
for workflow_text, step_name in ((workflow, "Prepare verified public Beads executable"),
                                 (remote_workflow, "Select opt-in public executable bundle")):
    step = workflow_text.split("- name: " + step_name, 1)[1].split("- name:", 1)[0]
    sha = re.findall(r"(?m)^ +TRUSTED_PREPARER_SHA: ([0-9a-f]{40})$", step)
    checksum = re.findall(r"(?m)^ +TRUSTED_PREPARER_SHA256: ([0-9a-f]{64})$", step)
    if len(sha) != 1 or len(checksum) != 1:
        raise SystemExit("config: helper admission needs literal immutable SHA and content hash")
    if ('prepare-public-bundle.py?ref=$TRUSTED_PREPARER_SHA' not in step
            or 'sha256sum --check --status' not in step
            or step.index('sha256sum --check --status') > step.index('prepare-public-bundle.py" request')):
        raise SystemExit("config: helper must be hash-checked before privileged invocation")
    helper_bindings.append((sha[0], checksum[0]))
if helper_bindings[0] != helper_bindings[1]:
    raise SystemExit("config: hosted and remote helper admission differs")
for step_name, workflow_text in (("Capture and run Plug Bazel lanes", workflow),
                                 ("Run remote Bazel command", remote_workflow)):
    step = workflow_text.split("- name: " + step_name, 1)[1].split("- name:", 1)[0]
    if "GH_TOKEN:" in step or "github.token" in step:
        raise SystemExit("config: read token must remain isolated from the test step")
workflow_descriptor = re.findall(r"(?m)^ +PUBLIC_BUNDLE_DESCRIPTOR: ([1-9][0-9]*:[1-9][0-9]*:[0-9a-f]{64})$", workflow)
collector_descriptor = next(ast.literal_eval(node.value) for node in ast.parse(collector).body
                            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                            and target.id == "PUBLIC_BUNDLE_DESCRIPTOR" for target in node.targets))
if workflow_descriptor != [collector_descriptor]:
    raise SystemExit("config: hosted descriptor and collector receipt descriptor disagree")

for phase in ("begin", "run", "finish"):
    if f"python3 scripts/ci/plug-bazel-delivery.py {phase}" not in workflow_code:
        raise SystemExit("config: workflow must invoke delivery phase " + phase)
for fragment in ("persist-credentials: false", "fetch-depth: 0", "${{ github.event.pull_request.head.sha || github.sha }}",
                 "if-no-files-found: error", "retention-days: 14", "--job-status \"$PRIOR_JOB_STATUS\"",
                 "if: github.event_name == 'push' && github.ref == 'refs/heads/main' && success()"):
    if fragment not in workflow_code:
        raise SystemExit("config: workflow lost trust/cache/upload guard: " + fragment)
for name in ("Stop Bazel and finalize delivery bundle", "Upload Plug CI evidence"):
    step = workflow_code.split("- name: " + name, 1)[1].split("- name:", 1)[0]
    if "if: ${{ always() }}" not in step:
        raise SystemExit("config: cleanup and upload must always execute")
if "|| true" in workflow_code or "|| true" in targets or "|| true" in collector:
    raise SystemExit("config: collection/query/shutdown failures must propagate")
actions = re.findall(r"uses: (\S+)", workflow_code)
if len(actions) != 6 or any(not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action) for action in actions):
    raise SystemExit("config: every Plug Bazel action must retain its full immutable pin")
if ".plug-bazel-evidence" in workflow or ".plug-bazel-targets" in workflow or ".plug-bazel-repository-cache" in workflow:
    raise SystemExit("config: CI runtime paths must stay outside the checked-out source tree")
for invalid in ("--no_system_rc", "--no_home_rc", "--no_workspace_rc"):
    if invalid in workflow or invalid in targets or invalid in collector:
        raise SystemExit(f"config: invalid Bazel startup option present: {invalid}")

def encoded(values):
    return "".join(chr(value) for value in values)

def without_comments(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

def require_fragments(label, code, fragments):
    missing = sorted(fragment for fragment in fragments if fragment not in code)
    if missing:
        raise SystemExit(f"config: {label} is missing: " + ", ".join(missing))

def yaml_blocks(code, key):
    """Return (indent, inline_value, child_lines) for each mapping named key."""
    lines = code.splitlines()
    blocks = []
    for index, line in enumerate(lines):
        match = re.match(rf"^( *)(?:{re.escape(key)})\s*:(?:\s*(.*))?$", line)
        if not match:
            continue
        indent = len(match.group(1))
        children = []
        for child in lines[index + 1:]:
            if child.strip():
                child_indent = len(child) - len(child.lstrip(" "))
                if child_indent <= indent:
                    break
            children.append(child)
        blocks.append((indent, (match.group(2) or "").strip(), children))
    return blocks

def immediate_keys(indent, children):
    keys = []
    for line in children:
        if not line.strip():
            continue
        if len(line) - len(line.lstrip(" ")) != indent + 2:
            continue
        match = re.match(
            r"^\s{2,}(?:(['\"])([A-Za-z0-9_-]+)\1|([A-Za-z0-9_-]+))\s*:(?:\s*(.*))?$",
            line,
        )
        if match:
            key = match.group(2) or match.group(3)
            keys.append((key, (match.group(4) or "").strip()))
    return keys

def check_dispatch_workflow(label, code, fragments, action_count, expected_triggers):
    """Check a workflow that may execute only trusted scheduled or dispatched code."""
    require_fragments(label, code, fragments)
    trigger_blocks = [block for block in yaml_blocks(code, "on") if block[0] == 0]
    if len(trigger_blocks) != 1 or trigger_blocks[0][1]:
        raise SystemExit(f"config: {label} must declare triggers as a mapping")
    trigger_keys = immediate_keys(trigger_blocks[0][0], trigger_blocks[0][2])
    if len(trigger_keys) != len({key for key, _ in trigger_keys}) or {
        key for key, _ in trigger_keys
    } != set(expected_triggers):
        raise SystemExit(f"config: {label} must be schedule/workflow_dispatch only")
    permission_blocks = yaml_blocks(code, "permissions")
    top_level_permissions = []
    for indent, inline_value, children in permission_blocks:
        if inline_value and inline_value != "{}":
            raise SystemExit(f"config: {label} permissions must be an explicit read/none mapping")
        entries = immediate_keys(indent, children)
        if len(entries) != len({key for key, _ in entries}):
            raise SystemExit(f"config: {label} contains duplicate permission keys")
        for key, value in entries:
            value = value.split(" #", 1)[0].strip()
            if value not in {"read", "none"}:
                raise SystemExit(f"config: {label} grants non-read permission {key}: {value}")
        if indent == 0:
            top_level_permissions.extend(entries)
    if dict(top_level_permissions).get("contents") != "read":
        raise SystemExit(f"config: {label} must grant top-level contents: read")
    if re.search(r"\bsecrets\b|github\.event\.(?:pull_request|issue|discussion)", code):
        raise SystemExit(f"config: {label} exposes a secret or untrusted event context")
    actions = re.findall(r"(?m)^\s*(?:uses|['\"]uses['\"])\s*:\s*(\S+)", code)
    if len(actions) != action_count or any(
        not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action)
        for action in actions
    ):
        raise SystemExit(f"config: every {label} action must retain its full immutable pin")
    # The expression contains spaces; compare the complete mapping line after
    # normalizing whitespace instead of trusting a substring in a run script.
    runson_lines = [re.sub(r"\s+", " ", line.strip()) for line in code.splitlines()
                    if re.match(r"^\s*runs-on:\s*", line)]
    if runson_lines != ["runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16"]:
        raise SystemExit(f"config: {label} must use the pinned RunsOn runner")

nightly_code = without_comments(nightly_workflow)
check_dispatch_workflow(
    "nightly restore-diffs workflow",
    nightly_code,
    (
        "name: Plug Nightly Restore Diffs", "schedule:", "workflow_dispatch:",
        "permissions:\n  contents: read",
        "runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16",
        "timeout-minutes: 45", "persist-credentials: false", "fetch-depth: 1",
        "actions/cache/restore@", "actions/upload-artifact@",
        "if-no-files-found: error", "retention-days: 30",
    ),
    4,
    ("schedule", "workflow_dispatch"),
)

if public_workflow is not None:
    public_code = without_comments(public_workflow)
    check_dispatch_workflow(
        "public bundle workflow",
        public_code,
        (
            "name: Plug public executable bundle", "workflow_dispatch:", "source_ref:",
            "permissions:\n  contents: read",
            "runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16",
            "timeout-minutes: 60", "persist-credentials: false", "fetch-depth: 0",
            "actions/upload-artifact@", "actions/download-artifact@",
            "if-no-files-found: error", "retention-days: 14",
            "public-bundle.py create", "public-bundle.py verify",
            "git fetch --no-tags origin refs/heads/beads",
            "ref: ${{ github.ref }}",
            'test "$(git rev-parse HEAD)" = "$GITHUB_SHA"',
            'test "$PLUG_COMMIT" = "$PLUG_COMPANION_COMMIT"',
            'test "$PLUG_SOURCE_REF" = "$PLUG_COMPANION_REF"',
            'test "$beads_tree" = "$BEADS_COMPANION_TREE"',
            'test "$plug_tree" = "$PLUG_COMPANION_TREE"',
            "--@rules_go//go/config:tags=gms_pure_go",
            '--expected-provenance "$RUNNER_TEMP/bundle-expected-provenance.json"',
            "Retain public companion download verification",
            "plug-public-bundle-verification-${{ inputs.label }}-${{ github.run_id }}",
            "public-download-verification.json",
            '[[ "$PLUG_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]',
            '[[ "$PLUG_SOURCE_REF" =~ ^[A-Za-z0-9._/-]+$ ]]',
            '[[ "$BUNDLE_LABEL" =~ ^[A-Za-z0-9._-]{1,48}$ ]]',
            '[[ "$PLUG_TARGET" =~ ^//[A-Za-z0-9_./:-]+$ ]]',
            "BEADS_COMMIT: 9d6fff760b7a314b476539198504d77ce0959656",
        ),
        5,
        ("workflow_dispatch",),
    )
    public_forbidden = (
        "pull_request_target:",
        encoded((103, 97, 115, 116, 111, 119, 110, 104, 97, 108, 108)) + "/" + encoded((98, 101, 97, 100, 115)),
        encoded((115, 116, 101, 118, 101, 121, 101, 103, 103, 101)) + "/" + encoded((98, 101, 97, 100, 115)),
        "rbe-west", "RBE_",
    )
    for token in public_forbidden:
        if token in public_code:
            raise SystemExit("config: public bundle workflow contains a forbidden token: " + token)

forbidden = (
    "pull_request_target:",
    "rbe-west",
    "RBE_",
    "RBE-",
    encoded((99, 109, 100)) + "/" + encoded((98, 100)),
    encoded((103, 97, 115)) + encoded((116, 111, 119, 110, 104, 97, 108, 108)),
    encoded((115, 116, 101, 118, 101, 121, 101, 103, 103, 101)) + "/" + encoded((98, 101, 97, 100, 115)),
)
for token in forbidden:
    if token in workflow or token in collector:
        raise SystemExit(f"config: inherited or privileged token present: {token}")

required_targets = ("rdeps(//...", "full_graph", "lockfile_mode=error", "//:lexicon_gate")
missing = sorted(fragment for fragment in required_targets if fragment not in targets)
if missing:
    raise SystemExit("config: target resolver is missing: " + ", ".join(missing))

remote_code = without_comments(remote_workflow)
if "workflow_dispatch:" not in remote_code or "  push:" in remote_code or "  pull_request:" in remote_code:
    raise SystemExit("config: remote Bazel workflow must be dispatch-only")
for fragment in (
    "run-name: remote-bazel ${{ inputs.label }} ${{ inputs.sha }}",
    "sha:", "label:", "args:", "type: string",
    "ref: ${{ inputs.sha }}", "fetch-depth: 1", "persist-credentials: false",
    "runs-on=${{ github.run_id }}/runner=bb-x64-16", "timeout-minutes: 180",
    "actions/cache/restore@", "actions/setup-node@", "node-version: 24",
    "command -v google-chrome", "read -ra USER_ARGS", "bazel \"${ARGS[@]}\"",
    "--ignore_all_rc_files", "--output_user_root", "--bazelrc",
    "remote-bazel-${{ inputs.label }}", "bazel.log", "exit_code",
    "toolchain.txt", "bazel-testlogs.tgz", "if: always()",
):
    if fragment not in remote_code:
        raise SystemExit("config: remote workflow is missing: " + fragment)
if "eval " in remote_code or "set -x" in remote_code or "pull_request_target:" in remote_code:
    raise SystemExit("config: remote workflow contains an unsafe command or trigger")
helper_checkout = remote_code.split("- name: Checkout trusted CI helpers", 1)[1].split("- name:", 1)[0]
require_fragments("trusted remote archive helper checkout", helper_checkout, (
    "if: always()", "ref: ${{ github.workflow_sha }}", "persist-credentials: false",
    "fetch-depth: 1", "sparse-checkout: |\n            scripts/ci/bazel-testlogs", "path: trusted-ci",
))
archive_step = remote_code.split("- name: Archive Bazel test logs", 1)[1].split("- name:", 1)[0]
require_fragments("trusted remote archive helper invocation", archive_step, (
    'python3 "$GITHUB_WORKSPACE/trusted-ci/scripts/ci/bazel-testlogs/archive.py"',
    '--testlogs bazel-testlogs --output "$archive"',
))
if not (remote_code.index("- name: Run remote Bazel command")
        < remote_code.index("- name: Checkout trusted CI helpers")
        < remote_code.index("- name: Archive Bazel test logs")):
    raise SystemExit("config: trusted helper checkout must follow Bazel and precede archiving")
remote_actions = re.findall(r"uses: (\S+)", remote_code)
if len(remote_actions) != 6 or any(not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action) for action in remote_actions):
    raise SystemExit("config: every remote Bazel action must retain its full immutable pin")

# Replay representative original CP7 markers through the workflow's actual
# preservation program. This is parser validation, never new latency evidence.
cp7_path = Path(".github/workflows/plug-cp7-latency.yml")
if cp7_path.exists():
    preservation = cp7_path.read_text().split("- name: Preserve raw measurements and XML evidence", 1)[1]
    program = textwrap.dedent(preservation.split("python3 - <<'PY'\n", 1)[1].split("\n          PY\n", 1)[0])
    # Latency: PR260 real run 41382713895, focused-final/bazel.log:281,305.
    # Dense: main run 42031345994, CP7 test.log:43,47 (Go-format counters).
    real_markers = {'CP7_CASE': '    latency_test.go:41: CP7_CASE {"engine":"sqlite","fraction_divisor":1,"operation":"Q4Ready","workload_sha256":"bac34d24657ee49bc1ec3659706a4870bcc9e87f574c1ebb9d7e95b4e1337116","corpus_sha256":"f1b2e7c76c581119829123a7f34d8a1441078c77f6b3f279169b4d5041073ff8","corpus":100000,"expected_authorized":100000,"cold_first_request":{"engine":"sqlite","fraction_divisor":1,"corpus":100000,"expected_authorized":100000,"operation":"Q4Ready","phase":"cold","index":0,"elapsed_ms":30000.077,"status":"request_deadline","error":"context deadline exceeded","reader_api":{"query_calls":17,"query_rows_returned":16000,"get_calls":0,"observed_keys":0},"target_ms":50,"target_pass":false,"validation_ms":0.096,"reset_ms":0},"warmups":3,"samples":20,"outcomes":{"request_deadline":20},"response_p95_ms":30003.836,"successful_latency_p95_ms":null,"target_ms":50,"target_pass":false,"status":"RED"}', 'CP7_SAMPLE': '    latency_test.go:81: CP7_SAMPLE {"engine":"sqlite","fraction_divisor":1,"corpus":100000,"expected_authorized":100000,"operation":"Q4Ready","phase":"cold","index":0,"elapsed_ms":30000.077,"status":"request_deadline","error":"context deadline exceeded","reader_api":{"query_calls":17,"query_rows_returned":16000,"get_calls":0,"observed_keys":0},"target_ms":50,"target_pass":false,"validation_ms":0.096,"reset_ms":0}', 'CP7_DENSE_ORDINARY': '    dense_summary_test.go:60: CP7_DENSE_ORDINARY authorized_inputs=100000 cap=20000 outcome=scope_too_large performance=RED', 'CP7_DENSE_TYPE_SUMMARY': '    dense_summary_test.go:84: CP7_DENSE_TYPE_SUMMARY fields_present=true fields_empty=true group_by=type finite_cp7=true authorized_inputs=100000 groups=1 corpus_sha256=7490011f3cd049ec7b254b35100f06896823f7eebd77bfc6979ad5e4e4a878cf executed_work={Payloads:10100000 PayloadBytes:2474499394 SelectorKeys:10000000 PrefixStates:0 PrefixBytes:0 AggregateInputs:100000 AggregateGroups:1 Projections:100000} performance=UNMEASURED'}
    case = json.loads(real_markers["CP7_CASE"].split("CP7_CASE ", 1)[1])
    ordinary = real_markers["CP7_DENSE_ORDINARY"].split("CP7_DENSE_ORDINARY ", 1)[1]
    summary = real_markers["CP7_DENSE_TYPE_SUMMARY"].split("CP7_DENSE_TYPE_SUMMARY ", 1)[1]

    def replay_cp7(engine, name, latency_lines, dense_lines, exit_code="0", xml='<testsuite><testcase name="control"/></testsuite>'):
        with tempfile.TemporaryDirectory(prefix="plug-cp7-parser.") as temporary:
            root = Path(temporary)
            evidence = root / "cp7-evidence"
            for target, lines in (("latency", latency_lines), ("dense_semantics", dense_lines)):
                target_dir = evidence / "test-results" / f"cp7_{target}_{engine}_test"
                target_dir.mkdir(parents=True)
                (target_dir / "test.log").write_text("\n".join(lines) + "\n")
                (target_dir / "test.xml").write_text(xml)
            (evidence / "exit_code").write_text(exit_code)
            result = subprocess.run([sys.executable, "-c", program], text=True, capture_output=True,
                                    env=dict(os.environ, RUNNER_TEMP=str(root), CP7_ENGINE=engine,
                                             GITHUB_STEP_SUMMARY=str(root / "step-summary")))
            accounting_path = evidence / "case-accounting.json"
            accounting = json.loads(accounting_path.read_text()) if accounting_path.exists() else None
            print(f"plug-cp7-parser: {engine} {name} exit={result.returncode}")
            return result, accounting

    dense_lines = [real_markers[kind] for kind in ("CP7_DENSE_ORDINARY", "CP7_DENSE_TYPE_SUMMARY")]
    for engine in ("sqlite", "postgres"):
        # Real marker replay stays partial: one original case/sample is no
        # 9/216 measurement matrix, and RED/UNMEASURED remain literal.
        result, accounting = replay_cp7(engine, "real-marker-partial", [real_markers["CP7_CASE"], real_markers["CP7_SAMPLE"]], dense_lines, "3")
        assert result.returncode == 0, result.stderr
        assert accounting["latency"]["raw_cases"] == 1 and accounting["latency"]["raw_samples"] == 1
        assert accounting["latency"]["cases"] == [case]
        assert case["status"] == "RED" and case["target_pass"] is False
        assert accounting["dense"]["ordinary"] == [ordinary]
        assert accounting["dense"]["type_summary"] == [summary]
        assert ordinary.endswith("performance=RED") and summary.endswith("performance=UNMEASURED")
        assert "PayloadBytes:2474499394" in summary and "AggregateInputs:100000" in summary
        assert accounting["dense"]["raw_ordinary_markers"] == accounting["dense"]["raw_summary_markers"] == 1

        # Repetition below is a synthetic count fixture, not a measured matrix.
        matrix = [real_markers["CP7_CASE"]] * 9 + [real_markers["CP7_SAMPLE"]] * 216
        result, accounting = replay_cp7(engine, "synthetic-count-matrix", matrix, dense_lines)
        assert result.returncode == 0, result.stderr
        assert accounting["latency"]["raw_cases"] == accounting["latency"]["expected_cases"] == 9
        assert accounting["latency"]["raw_samples"] == accounting["latency"]["expected_samples"] == 216
        assert all(record == case for record in accounting["latency"]["cases"])
        for name, latency_input, dense_input, expected_error in (
            ("missing-json-markers", [], dense_lines, "declared 9/216 matrix"),
            ("missing-dense-summary", matrix, dense_lines[:1], "both semantic controls"),
            ("duplicate-dense-marker", matrix, dense_lines + dense_lines[:1], "both semantic controls"),
        ):
            result, accounting = replay_cp7(engine, name, latency_input, dense_input)
            assert result.returncode != 0 and expected_error in result.stderr
            assert accounting is not None  # Counts survive a failed completeness check.
        for kind in ("CP7_CASE", "CP7_SAMPLE"):
            result, accounting = replay_cp7(engine, "malformed-" + kind, matrix + [kind + " {broken"], dense_lines)
            assert result.returncode != 0 and "JSONDecodeError" in result.stderr
        result, accounting = replay_cp7(engine, "outer-xml-failure", matrix, dense_lines,
                                       xml='<testsuite><testcase name="control"><failure/></testcase></testsuite>')
        assert result.returncode != 0 and "passing outer XML" in result.stderr
        assert accounting["dense"]["xml"]["failures"] == 1
    print("plug-cp7-parser: real JSON, raw dense counters, literal RED/UNMEASURED and malformed/missing controls passed")

check_workflow_inventory(Path(".github/workflows"))
print("plug-ci-config: workflow, permissions, cache, lock, targets, and retirement checks passed")
PYTHON

if "$targets" --help >/dev/null; then :; else
  echo "config: target resolver help failed" >&2
  exit 1
fi
if "$targets" --unknown --output "${TMPDIR:-/tmp}/plug-ci-invalid" >/dev/null 2>&1; then
  echo "config: target resolver accepted an unknown option" >&2
  exit 1
fi

"$target_tests"
"$delivery_tests"
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/public-bundle-test.py

echo "plug-ci-config: shell syntax and fail-closed argument checks passed"
