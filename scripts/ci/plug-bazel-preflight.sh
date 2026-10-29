#!/usr/bin/env bash
# Plug's pinned, fresh Bazel preflight. The supervisor owns every descendant,
# enforces the work deadline independently of Bash, and freezes only stopped work.
set -euo pipefail
if [[ "${1:-}" == --plug-bazel-worker &&
  "${PLUG_BAZEL_PRIVATE_PARENT:-}" == "$PPID" ]]; then
  shift
else
  exec python3 - "$0" "$@" <<'PLUG_SUPERVISOR'
import ctypes
import hashlib
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

script, *arguments = sys.argv[1:]
limit_text = os.environ.get('PLUG_BAZEL_TIMEOUT_SECONDS', '2400')
evidence = Path(os.environ.get('PLUG_BAZEL_EVIDENCE_DIR') or tempfile.mkdtemp(prefix='plug-bazel-evidence.')).absolute()
evidence.mkdir(parents=True, exist_ok=True)
if any(evidence.iterdir()):
    sys.exit('plug-bazel-preflight: evidence directory must be empty; refusing to change a prior snapshot')
scratch = Path(tempfile.mkdtemp(prefix='plug-bazel-bootstrap.'))
started = time.monotonic()
started_epoch = int(time.time())
received = []
for number in (signal.SIGINT, signal.SIGTERM):
    signal.signal(number, lambda sig, frame: received.append(sig))
# Orphaned grandchildren remain ours even if their command/session leader exits
# between polls. This owns only descendants of this supervisor, never siblings.
history = {}
worker = None
status = 1
reason = 'exit'
termination_signal = 'none'
deadline_reached = False
snapshot_complete = False
shutdown_complete = False


def event(message):
    line = f'{time.time():.3f} {message}\n'
    print('plug-bazel-preflight: ' + message, flush=True)
    with (evidence / 'lifecycle.txt').open('a') as out:
        out.write(line)


def processes():
    table = {}
    for item in Path('/proc').iterdir():
        if not item.name.isdecimal():
            continue
        try:
            raw = (item / 'stat').read_text()
            fields = raw.rsplit(') ', 1)[1].split()
            table[int(item.name)] = dict(pid=int(item.name), ppid=int(fields[1]),
                pgid=int(fields[2]), sid=int(fields[3]), state=fields[0],
                start_ticks=int(fields[19]), command=raw.split('(', 1)[1].rsplit(') ', 1)[0])
        except (OSError, ValueError, IndexError):
            continue
    owned = {os.getpid()}
    # Keep identities after parent exit, including separate process groups.
    owned.update(pid for (pid, birth) in history
                 if pid in table and table[pid]['start_ticks'] == birth)
    while True:
        expanded = owned | {pid for pid, row in table.items() if row['ppid'] in owned}
        if expanded == owned:
            break
        owned = expanded
    result = {pid: table[pid] for pid in owned if pid != os.getpid() and pid in table}
    for row in result.values():
        history[(row['pid'], row['start_ticks'])] = row
    return result


def reap(table):
    if worker is not None:
        worker.poll()
    for pid, row in table.items():
        if row['state'] == 'Z' and (worker is None or pid != worker.pid):
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass


def live():
    table = processes()
    reap(table)
    return {pid: row for pid, row in processes().items() if row['state'] != 'Z'}


def send(table, sig, end, escalated):
    privileged = []
    for pid, row in table.items():
        if time.monotonic() >= end:
            break
        # Recheck start time before a signal; a recycled PID is not our process.
        try:
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
            if int(fields[19]) != row['start_ticks']:
                continue
        except (OSError, ValueError, IndexError):
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            # Only an identified descendant of opted-in sudo cleanup can need
            # this. Never signal by command name, user, or an unowned group.
            if (pid, row['start_ticks'], sig) not in escalated:
                privileged.append(str(pid))
                escalated.add((pid, row['start_ticks'], sig))
    if privileged and time.monotonic() < end:
        # This helper is owned too. Never use subprocess.run/wait here: even a
        # timed-out synchronous wait could block shutdown on an unkillable child.
        try:
            subprocess.Popen(['sudo', '--non-interactive', '/bin/kill',
                              f'-{sig}', '--', *privileged], stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True)
        except OSError:
            pass


def stop_owned():
    stop_started = time.monotonic()
    escalated = set()
    for sig, seconds in ((signal.SIGINT, 3), (signal.SIGTERM, 3), (signal.SIGKILL, 4)):
        table = live()
        if not table:
            break
        event(f'shutdown signal={signal.Signals(sig).name} processes={len(table)}')
        stage_end = min(stop_started + 12, time.monotonic() + seconds)
        while table and time.monotonic() < stage_end:
            send(table, sig, stage_end, escalated)
            if time.monotonic() >= stage_end:
                break
            time.sleep(.05)
            table = live()
        if not table:
            break
    return not live()


def inventory(name, rows):
    with (evidence / name).open('w') as out:
        out.write('pid ppid pgid sid state start_ticks command\n')
        for row in sorted(rows, key=lambda x: (x['pid'], x['start_ticks'])):
            out.write('{pid} {ppid} {pgid} {sid} {state} {start_ticks} {command}\n'.format(**row))


def record_shutdown():
    # Retain historical identities, including finalization helpers. An absent
    # process with a recycled PID must never be reported as the owned process.
    current = processes()
    rows = []
    for (pid, birth), old in history.items():
        row = dict(old)
        now = current.get(pid)
        row['state'] = now['state'] if now and now['start_ticks'] == birth else 'absent'
        rows.append(row)
    inventory('processes-after-shutdown.txt', rows)
    (evidence / 'process-shutdown.txt').write_text(
        f'owned_processes_observed={len(history)}\n'
        f'owned_writers_running={str(not shutdown_complete).lower()}\n')


def bounded(action, end):
    # Copy/hash work has its own finite budget too. A blocked filesystem call
    # cannot leave the main supervisor waiting indefinitely in an EXIT trap.
    child = os.fork()
    if child == 0:
        os.setsid()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, signal.SIG_DFL)
        try:
            action()
        except BaseException as exc:
            print(f'plug-bazel-preflight: finalization failed: {type(exc).__name__}: {exc}', flush=True)
            os._exit(1)
        os._exit(0)
    while time.monotonic() < end:
        done, result = os.waitpid(child, os.WNOHANG)
        if done:
            complete = os.waitstatus_to_exitcode(result) == 0 and not live()
            if not complete:
                stop_owned()
            return complete
        processes()  # Retain the helper's identity and any descendants as well.
        time.sleep(.05)
    event('finalization budget exhausted')
    stop_owned()
    return False


def copy_evidence():
    for name in ('bazel-version.txt', 'bazel-release.txt', 'go-sdk.txt',
                 'bazel-testlogs.txt', 'build-command.txt', 'test-command.txt',
                 'build-events.json', 'test-events.json', 'build.log', 'test.log',
                 'owned-processes.txt', 'plug.bazelrc', 'phase.txt'):
        source = scratch / name
        if source.is_file():
            shutil.copyfile(source, evidence / name)
    testlogs_file = scratch / 'testlogs-root.txt'
    if testlogs_file.is_file():
        testlogs = Path(testlogs_file.read_text().strip())
        output_root = (scratch / 'bazel-output').resolve()
        if not testlogs.resolve().is_relative_to(output_root):
            raise ValueError('test XML escaped the owned output root')
        visited = set()
        for directory, directories, files in os.walk(testlogs, followlinks=True):
            directory = Path(directory)
            identity = (directory.stat().st_dev, directory.stat().st_ino)
            if identity in visited:
                directories[:] = []
                continue
            visited.add(identity)
            if 'test.xml' in files:
                source = directory / 'test.xml'
                if not source.resolve().is_relative_to(output_root):
                    raise ValueError('test XML escaped the owned output root')
                target = evidence / 'test-xml' / source.relative_to(testlogs)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
    disk = os.statvfs(scratch)
    (evidence / 'disk-after-preflight.txt').write_text(
        f'path={scratch}\nblock_size={disk.f_frsize}\n'
        f'available_bytes={disk.f_bavail * disk.f_frsize}\n'
        f'total_bytes={disk.f_blocks * disk.f_frsize}\n')


def write_result(frozen):
    phase_file = scratch / 'phase.txt'
    phase = phase_file.read_text().strip() if phase_file.is_file() else 'initializing'
    (evidence / 'preflight-result.txt').write_text(
        f'exit_status={status}\nphase={phase or "unknown"}\n'
        f'termination_signal={termination_signal}\ntermination_reason={reason}\n'
        f'deadline_reached={str(deadline_reached).lower()}\ntimeout_seconds={limit_text}\n'
        f'owned_shutdown_complete={str(shutdown_complete).lower()}\n'
        f'snapshot_complete={str(snapshot_complete).lower()}\n'
        f'evidence_frozen={str(frozen).lower()}\nbazel_mode=batch\n')


def make_manifest():
    # The temporary manifest shares the destination filesystem, including when
    # evidence is a separate mount. Exclude only this unpublished temp file.
    with (evidence / '.manifest.sha256.tmp').open('w') as out:
        for path in sorted(evidence.rglob('*')):
            if path == evidence / '.manifest.sha256.tmp':
                continue
            if path.is_symlink():
                raise ValueError('live symlink in evidence')
            if path.is_file():
                with path.open('rb') as source:
                    digest = hashlib.file_digest(source, 'sha256').hexdigest()
                out.write(f'{digest}  ./{path.relative_to(evidence).as_posix()}\n')

try:
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError('cannot establish descendant ownership')
    for name in ('BAZEL_REMOTE_EXECUTOR', 'BAZEL_FORK_CACHE', 'BAZEL_FORK_REMOTE',
                 'BAZEL_RRC_READ', 'RBE_WEST_EXECUTOR', 'RBE_TLS_CERT',
                 'RBE_TLS_KEY', 'RBE_TLS_CA', 'RBE_INSTANCE'):
        if os.environ.get(name):
            raise ValueError(name + ' must be unset for the Plug preflight')
    if not limit_text.isascii() or not limit_text.isdecimal() or not 1 <= int(limit_text) <= 2400:
        raise ValueError('PLUG_BAZEL_TIMEOUT_SECONDS must be an integer from 1 to 2400')
    limit = int(limit_text)
    (evidence / 'deadline.txt').write_text(
        f'timeout_seconds={limit}\nouter_job_seconds=2700\nreserved_seconds={2700-limit}\n'
        'shutdown_budget_seconds=12\nfinalization_budget_seconds=90\n')
    event(f'supervisor started pid={os.getpid()} python={sys.version.split()[0]}')
    environment = os.environ.copy()
    environment.update(PLUG_BAZEL_PRIVATE_PARENT=str(os.getpid()),
                       PLUG_BAZEL_PRIVATE_WORK_DIR=str(scratch),
                       PLUG_BAZEL_PRIVATE_EVIDENCE_DIR=str(evidence))
    worker = subprocess.Popen(['bash', script, '--plug-bazel-worker', *arguments],
                              env=environment, start_new_session=True)
    event(f'worker started pid={worker.pid}')
    while True:
        processes()
        code = worker.poll()
        if received:
            sig = received[0]
            status, reason, termination_signal = 128 + sig, 'signal', signal.Signals(sig).name.removeprefix('SIG')
            break
        if time.monotonic() - started >= limit:
            status, reason, termination_signal, deadline_reached = 124, 'internal-deadline', 'deadline', True
            break
        if code is not None:
            status = code if code >= 0 else 128 - code
            if status == 0 and live():
                status, reason = 1, 'surviving-descendants'
            break
        reap(processes())
        time.sleep(.05)
except (OSError, ValueError) as exc:
    event(str(exc))
    status = 1

shutdown_started = time.monotonic()
shutdown_epoch = int(time.time())
event(f'work ended status={status} reason={reason}; stopping owned descendants')
inventory('processes-before-shutdown.txt', processes().values())
shutdown_complete = stop_owned()
shutdown_seconds = time.monotonic() - shutdown_started
record_shutdown()
if not shutdown_complete:
    status = status or 1
    event('owned descendants did not stop; refusing raw snapshot and frozen certificate')
else:
    event('owned descendants stopped; copying available evidence')
    final_end = shutdown_started + 90
    snapshot_complete = bounded(copy_evidence, final_end)
    if not snapshot_complete:
        status = status or 1
    shutdown_complete = shutdown_complete and not live()
    record_shutdown()
if received and reason != 'signal':
    sig = received[0]
    status, reason, termination_signal = 128 + sig, 'signal', signal.Signals(sig).name.removeprefix('SIG')
(evidence / 'finalization.txt').write_text(
    f'started_epoch={started_epoch}\nshutdown_started_epoch={shutdown_epoch}\n'
    f'snapshot_completed_epoch={int(time.time())}\nelapsed_seconds={time.monotonic()-started:.3f}\n'
    f'shutdown_seconds={shutdown_seconds:.3f}\n'
    f'snapshot_seconds={time.monotonic()-shutdown_started-shutdown_seconds:.3f}\n')
frozen = shutdown_complete and snapshot_complete and not live()
write_result(frozen)
if frozen:
    event('snapshot complete; hashing immutable regular files')
    signal_count = len(received)
    if bounded(make_manifest, final_end) and len(received) == signal_count:
        try:
            os.replace(evidence / '.manifest.sha256.tmp', evidence / 'manifest.sha256')
        except OSError as exc:
            event(f'manifest publication failed: {exc}')
            frozen = False
    else:
        frozen = False
    if not frozen:
        shutdown_complete = shutdown_complete and not live()
        record_shutdown()
        if received:
            sig = received[0]
            status, reason, termination_signal = 128 + sig, 'signal', signal.Signals(sig).name.removeprefix('SIG')
        else:
            status = status or 1
        (evidence / '.manifest.sha256.tmp').unlink(missing_ok=True)
        write_result(False)
print(f'plug-bazel-preflight: finalization complete status={status} frozen={str(frozen).lower()} elapsed={time.monotonic()-started:.3f}s', flush=True)
sys.exit(status)
PLUG_SUPERVISOR
fi

usage() {
  cat <<'EOF'
usage: scripts/ci/plug-bazel-preflight.sh [--targets '<patterns>']

Run a fresh local Bazel build and test using the Bazel version in .bazelversion.
The optional --targets value is a shell-word list (default: //...). Evidence is
written to PLUG_BAZEL_EVIDENCE_DIR, or to a temporary directory when unset.
EOF
}

targets_text="${PLUG_BAZEL_TARGETS:-//...}"
while (($# > 0)); do
  case "$1" in
    --targets)
      (($# >= 2)) || { echo "plug-bazel-preflight: --targets needs a value" >&2; exit 2; }
      targets_text="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "plug-bazel-preflight: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

read -r -a targets <<<"$targets_text"
((${#targets[@]} > 0)) || { echo "plug-bazel-preflight: target list is empty" >&2; exit 2; }

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

evidence_dir="${PLUG_BAZEL_PRIVATE_EVIDENCE_DIR:?missing supervisor evidence directory}"
work_dir="${PLUG_BAZEL_PRIVATE_WORK_DIR:?missing supervisor work directory}"
printf 'initializing\n' >"$work_dir/phase.txt"
fail() {
  echo "plug-bazel-preflight: $*" >&2
  exit 1
}

# A clean Plug run has no path to a remote executor or remote action cache.
# Refuse a contaminated environment instead of silently producing ambiguous
# evidence. Values are never printed.
for name in \
  BAZEL_REMOTE_EXECUTOR \
  BAZEL_FORK_CACHE \
  BAZEL_FORK_REMOTE \
  BAZEL_RRC_READ \
  RBE_WEST_EXECUTOR \
  RBE_TLS_CERT \
  RBE_TLS_KEY \
  RBE_TLS_CA \
  RBE_INSTANCE; do
  if [[ -n "${!name:-}" ]]; then
    fail "$name must be unset for the Plug preflight"
  fi
done

df -B1 -- "$evidence_dir" >"$evidence_dir/disk-before-preflight.txt"
case "${PLUG_BAZEL_CLEAN_HOSTED_RUNNER:-}" in
  '') ;;
  1)
    [[ "${GITHUB_ACTIONS:-}" == true &&
      "${RUNNER_ENVIRONMENT:-}" == github-hosted &&
      "${RUNNER_OS:-}" == Linux ]] || fail "toolchain cleanup requires an ephemeral GitHub-hosted Linux runner"
    phase=runner-cleanup
    printf '%s\n' "$phase" >"$work_dir/phase.txt"
    cp -- "$evidence_dir/disk-before-preflight.txt" "$evidence_dir/disk-before-cleanup.txt"
    # Fixed allowlist of unused image toolchains. Keep the OS, Docker, Node and
    # Python; Bazel and Go come from the verified pins below, not these paths.
    for toolchain in /usr/local/lib/android /usr/share/dotnet /usr/local/.ghcup \
      /usr/local/swift /usr/share/swift /opt/ghc /opt/hostedtoolcache/CodeQL; do
      if [[ -e "$toolchain" ]]; then
        sudo --non-interactive du -sx -B1 -- "$toolchain" \
          | tee -a "$evidence_dir/removed-toolchains.txt"
        sudo --non-interactive rm --recursive --force --one-file-system -- "$toolchain"
      fi
    done
    df -B1 -- "$evidence_dir" | tee "$evidence_dir/disk-after-cleanup.txt"
    free_bytes="$(df -B1 --output=avail -- "$evidence_dir" | tail -1)"
    ((free_bytes >= 30 * 1024 * 1024 * 1024)) || fail "less than 30 GiB free after hosted toolchain cleanup"
    ;;
  *) fail "PLUG_BAZEL_CLEAN_HOSTED_RUNNER must be 1 or unset" ;;
esac

[[ -f .bazelversion ]] || fail "missing .bazelversion"
bazel_version="$(tr -d '[:space:]' < .bazelversion)"
[[ "$bazel_version" == "9.3.0" ]] || fail "unsupported .bazelversion '$bazel_version'; add its verified digest before changing the pin"

gms_pure_go_mode=not-required
case "${PLUG_BAZEL_GMS_PURE_GO:-}" in
  1|true)
    gms_pure_go_mode=workflow-required-explicit
    ;;
  '')
    if grep -Eq 'com_github_dolthub_go_mysql_server|go-mysql-server' \
      MODULE.bazel gazelle.go.work go.mod; then
      if [[ ! -f .bazelrc ]] || ! grep -Eq \
        '^[[:space:]]*build([:[:space:]]|:).*--@rules_go//go/config:tags=[^#]*gms_pure_go' \
        .bazelrc; then
        fail "go-mysql-server is present but the inherited gms_pure_go declaration is missing"
      fi
      gms_pure_go_mode=inherited-required-explicit
    fi
    ;;
  *)
    fail "PLUG_BAZEL_GMS_PURE_GO must be 1 or unset"
    ;;
esac

case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    bazelisk_arch=amd64
    bazelisk_sha=5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992
    bazel_sha=d302d22ed77ee5658b87857249baa579db84300ecebe0a1e9a357b5b20f80eb8
    ;;
  Linux:aarch64|Linux:arm64)
    bazelisk_arch=arm64
    bazelisk_sha=e20e8b0f4f240091b7a55bf17b9398bd4f40ee70ae0208dff95dd4c445fb4010
    bazel_sha=615cc2509819047e4930fe6643c5f01ebd705ccadf75e8f0b85f9b83cc6a6ca0
    ;;
  *)
    fail "unsupported runner $(uname -s)/$(uname -m); the bootstrap currently targets Linux"
    ;;
esac

# Bazel's sandbox can leave root-owned files in this directory on the local
# testbox. Keep it for diagnostics; CI runner cleanup removes it after the job.

bazelisk="$work_dir/bazelisk"
curl -fsSL --retry 3 --max-time 120 \
  -o "$bazelisk" \
  "https://github.com/bazelbuild/bazelisk/releases/download/v1.29.0/bazelisk-linux-${bazelisk_arch}"
printf '%s  %s\n' "$bazelisk_sha" "$bazelisk" | /usr/bin/sha256sum -c -
chmod 755 "$bazelisk"

export BAZELISK_HOME="$work_dir/bazelisk-home"
export BAZELISK_VERIFY_SHA256="$bazel_sha"
mkdir -p "$BAZELISK_HOME"

rc="$work_dir/plug.bazelrc"
cat >"$rc" <<'EOF'
# Generated by plug-bazel-preflight.sh. It intentionally has no workspace rc.
common --enable_bzlmod
common --color=no
common --incompatible_strict_action_env
common --repo_contents_cache=
common --remote_cache=
common --remote_executor=
test --test_output=errors
test --test_env=GO_TEST_WRAP_TESTV=1
EOF
if [[ "$gms_pure_go_mode" == inherited-required-explicit ||
  "$gms_pure_go_mode" == workflow-required-explicit ]]; then
  printf '%s\n' 'build --@rules_go//go/config:tags=gms_pure_go' >>"$rc"
fi

output_root="$work_dir/bazel-output"
startup=(
  --batch
  --nosystem_rc
  --nohome_rc
  --noworkspace_rc
  "--bazelrc=$rc"
  "--output_user_root=$output_root"
)

run_bazel() {
  local log="$1" status invocation_pid output_pid
  shift
  # The supervisor retains process identities and adopts surviving descendants
  # across each command return. These PIDs are diagnostic, never sole ownership.
  setsid --wait env --default-signal=INT,TERM \
    "$bazelisk" "${startup[@]}" "$@" >"$log" 2>&1 &
  invocation_pid=$!
  printf 'pid=%s command=%s\n' "$invocation_pid" "$1" >>"$work_dir/owned-processes.txt"
  tail --pid="$invocation_pid" --sleep-interval=0.1 -n +1 -f "$log" &
  output_pid=$!
  if wait "$invocation_pid"; then status=0; else status=$?; fi
  wait "$output_pid" || :
  return "$status"
}

run_bazel "$work_dir/bazel-version.txt" version --gnu_format
grep -Fq "bazel ${bazel_version}" "$work_dir/bazel-version.txt" || fail "Bazelisk did not resolve Bazel $bazel_version"

run_bazel "$work_dir/bazel-release.txt" info release
grep -Fxq "release $bazel_version" "$work_dir/bazel-release.txt" || fail "Bazel release is not $bazel_version"

run_bazel "$work_dir/bazel-testlogs.txt" info bazel-testlogs
testlogs_root="$(tail -1 "$work_dir/bazel-testlogs.txt")"
[[ "$testlogs_root" == "$output_root/"* ]] || fail "Bazel test logs escaped the owned output root"
printf '%s\n' "$testlogs_root" >"$work_dir/testlogs-root.txt"

run_bazel "$work_dir/go-sdk.txt" cquery @go_sdk//:go_sdk --output=build
go_sdk_version="$(sed -n 's/^  version = "\([^"]*\)".*/\1/p' "$work_dir/go-sdk.txt" | head -1)"
[[ -n "$go_sdk_version" ]] || fail "could not resolve the Bazel Go SDK version"

if command -v nix >/dev/null 2>&1; then
  nix_version="$(nix --version 2>&1)"
else
  nix_version="unavailable on runner"
fi

source_digest="$(git ls-files -z | xargs -0 /usr/bin/sha256sum | LC_ALL=C sort -k2 | /usr/bin/sha256sum | awk '{print $1}')"
generator_inputs=(.bazelversion MODULE.bazel MODULE.bazel.lock)
for generator_input in .bazelrc .bazelignore .golangci.yml go.mod go.sum gazelle.go.work \
  tools/nogo/go.mod tools/nogo/go.sum tools/nogo/config.json third_party/patches/*.patch; do
  [[ -f "$generator_input" ]] && generator_inputs+=("$generator_input")
done
generator_digest="$(/usr/bin/sha256sum "${generator_inputs[@]}" | /usr/bin/sha256sum | awk '{print $1}')"
runner_class="${PLUG_RUNNER_CLASS:-github-hosted-ubuntu}"

cat >"$evidence_dir/preflight.txt" <<EOF
bazelisk_version=v1.29.0
bazel_version=$bazel_version
nix_version=$nix_version
go_sdk_version=$go_sdk_version
runner_class=$runner_class
remote_executor=disabled
remote_action_cache=disabled
test_result_cache=fresh
gms_pure_go_mode=$gms_pure_go_mode
source_digest=$source_digest
generator_digest=$generator_digest
EOF
printf '%s\n' "$targets_text" >"$evidence_dir/targets.txt"

build_bep="$work_dir/build-events.json"
test_bep="$work_dir/test-events.json"
build_command=("${startup[@]}" build "${targets[@]}" "--build_event_json_file=$build_bep" --show_result=0)
test_command=("${startup[@]}" test "${targets[@]}" "--build_event_json_file=$test_bep" --nocache_test_results --test_output=errors)

printf '%q ' "$bazelisk" "${build_command[@]}" | sed 's/ $//' >"$work_dir/build-command.txt"
printf '\n' >>"$work_dir/build-command.txt"
printf '%q ' "$bazelisk" "${test_command[@]}" | sed 's/ $//' >"$work_dir/test-command.txt"
printf '\n' >>"$work_dir/test-command.txt"

phase=build
printf '%s\n' "$phase" >"$work_dir/phase.txt"
echo "plug-bazel-preflight: bazel build $targets_text"
run_bazel "$work_dir/build.log" build "${targets[@]}" "--build_event_json_file=$build_bep" --show_result=0

phase="test"
printf '%s\n' "$phase" >"$work_dir/phase.txt"
echo "plug-bazel-preflight: bazel test $targets_text (fresh results)"
run_bazel "$work_dir/test.log" test "${targets[@]}" "--build_event_json_file=$test_bep" --nocache_test_results --test_output=errors

phase=complete
printf '%s\n' "$phase" >"$work_dir/phase.txt"
echo "plug-bazel-preflight: evidence=$evidence_dir"
echo "plug-bazel-preflight: bazel=$bazel_version go_sdk=$go_sdk_version runner=$runner_class"
