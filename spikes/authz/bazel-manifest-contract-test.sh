#!/usr/bin/env bash

# Bazel acceptance for the real nested module, using only the declared SDK
# and checksum-pinned file proxy. Every Go cache starts empty. Network system
# calls are denied even on hosts using processwrapper-sandbox. Benchmark
# commands remain guarded fakes throughout the contract harness.

set -euo pipefail

# This host may prohibit network namespaces. Install an inherited Linux
# seccomp filter instead; fail closed if it cannot be installed. This is
# deliberately limited to the lock's linux/amd64 execution platform.
if [[ "${1:-}" != --network-isolated ]]; then
  exec python3 - "$0" "$@" <<'PY'
import ctypes
import errno
import os
import platform
import sys

if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
    raise SystemExit("authz offline acceptance requires linux/amd64")


class Filter(ctypes.Structure):
    _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte),
                ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint32)]


class Program(ctypes.Structure):
    _fields_ = [("length", ctypes.c_ushort), ("filters", ctypes.POINTER(Filter))]


# Validate the syscall architecture, refuse x32, then deny every amd64
# socket/network syscall. File access and ordinary compiler subprocesses work.
deny = 0x00050000 | errno.EPERM
instructions = [(0x20, 0, 0, 4), (0x15, 1, 0, 0xC000003E),
                (0x06, 0, 0, 0x80000000), (0x20, 0, 0, 0),
                (0x35, 0, 1, 0x40000000), (0x06, 0, 0, deny)]
for syscall in [41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
                288, 299, 307]:
    instructions.extend([(0x15, 0, 1, syscall), (0x06, 0, 0, deny)])
instructions.append((0x06, 0, 0, 0x7FFF0000))
filters = (Filter * len(instructions))(*(Filter(*row) for row in instructions))
program = Program(len(instructions), filters)
libc = ctypes.CDLL(None, use_errno=True)
libc.prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
for operation, value in [(38, 1), (22, ctypes.addressof(program))]:
    # PR_SET_NO_NEW_PRIVS; PR_SET_SECCOMP with SECCOMP_MODE_FILTER.
    args = (operation, value, 0, 0, 0) if operation == 38 else (22, 2, value, 0, 0)
    if libc.prctl(*args) != 0:
        raise OSError(ctypes.get_errno(), "cannot install authz network denial")
os.execv("/bin/bash", ["bash", sys.argv[1], "--network-isolated", *sys.argv[2:]])
PY
fi
shift
python3 - <<'PY'
import errno
import socket

for family in (socket.AF_INET, socket.AF_INET6):
    try:
        connection = socket.socket(family, socket.SOCK_STREAM)
    except OSError as error:
        if error.errno != errno.EPERM:
            raise
    else:
        connection.close()
        raise SystemExit("authz acceptance network denial is ineffective")
print("offline execution: IPv4 and IPv6 socket syscalls denied (EPERM)")
PY

go_runfile="${1:?expected the registered Go SDK runfile path}"
proxy_runfile="${2:?expected the declared proxy ROOT runfile path}"
runfiles="${TEST_SRCDIR:?}/$TEST_WORKSPACE"
source_dir="$runfiles/spikes/authz"
checkout="$TEST_TMPDIR/authz-checkout"
go_bin="$TEST_SRCDIR/$go_runfile"
proxy_marker="$TEST_SRCDIR/$proxy_runfile"
proxy_dir="$(dirname "$proxy_marker")"
output_dir="${TEST_UNDECLARED_OUTPUTS_DIR:?}"

test -x "$go_bin"
test -f "$proxy_marker"
mkdir -p "$output_dir" "$TEST_TMPDIR/contract-tmp"
export TMPDIR="$TEST_TMPDIR/contract-tmp"

# Ignore ambient module selection, caches, private-module settings and
# automatic toolchain downloads. Only declared file-proxy bytes are usable.
export GOROOT="$(dirname "$(dirname "$go_bin")")"
export GOPROXY="file://$proxy_dir" GOSUMDB=off GOWORK=off GOENV=off
export GOTOOLCHAIN=local GO111MODULE=on GOFLAGS=-mod=readonly GOVCS='*:off'
export GOPRIVATE= GONOPROXY= GONOSUMDB= GOEXPERIMENT=
export CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1
export GOMODCACHE="$TEST_TMPDIR/main-caches/modules"
export GOCACHE="$TEST_TMPDIR/main-caches/build" GOPATH="$TEST_TMPDIR/main-caches/path"
mkdir "$TEST_TMPDIR/main-caches"
mkdir "$GOMODCACHE" "$GOCACHE" "$GOPATH"
test -z "$(find "$TEST_TMPDIR/main-caches" -mindepth 2 -print -quit)"
test "$("$go_bin" version)" = 'go version go1.26.10 linux/amd64'

python3 - "$source_dir" "$proxy_dir" "$output_dir/dependency-inputs.json" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

source, proxy, output = map(Path, sys.argv[1:])
lock_bytes = (source / "contract-modules.lock.json").read_bytes()
assert (proxy / "ROOT").read_bytes() == lock_bytes, "proxy ROOT differs from declared lock"
lock = json.loads(lock_bytes)
assert lock["schema"] == "plug-authz-contract-modules/v1"
assert lock["toolchain"] == {"version": "go1.26.10", "goos": "linux", "goarch": "amd64"}
assert lock["source"]["module"] == "github.com/buildbetter-app/plugs/spikes/authz"
for name, field in [("go.mod", "go_mod_sha256"), ("go.sum", "go_sum_sha256")]:
    assert hashlib.sha256((source / name).read_bytes()).hexdigest() == lock["source"][field]
expected = set()
for line in (source / "go.sum").read_text().splitlines():
    path, version, _ = line.split()
    extension = "mod" if version.endswith("/go.mod") else "zip"
    expected.add((path, version.removesuffix("/go.mod"), extension))
actual = set()
inventory = []
for module in lock["modules"]:
    escape = lambda value: "".join("!" + c.lower() if c.isupper() else c for c in value)
    relative = f'{escape(module["path"])}/@v/{escape(module["version"])}'
    for extension in ("mod", "zip"):
        field = extension + "_sha256"
        if field not in module:
            continue
        member = relative + "." + extension
        digest = hashlib.sha256((proxy / member).read_bytes()).hexdigest()
        assert digest == module[field], f"declared proxy checksum mismatch: {member}"
        actual.add((module["path"], module["version"], extension))
        inventory.append({"path": member, "sha256": digest})
    assert json.loads((proxy / (relative + ".info")).read_text()) == {"Version": module["version"]}
assert actual == expected, "proxy lock does not cover the unchanged go.sum"
output.write_text(json.dumps({"lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
                              "source": lock["source"], "toolchain": lock["toolchain"],
                              "initial_caches_empty": True, "network_denied": True,
                              "files": inventory}, indent=2) + "\n")
print(f"declared proxy verified: {len(lock['modules'])} modules, {len(inventory)} hashed files")
PY

mkdir -p "$checkout/spikes/authz"
# Runfiles entries are commonly symlinks. Go's //go:embed rejects an irregular
# (symlinked) file, so dereference the declared module inputs into the private
# checkout before invoking the nested module.
cp -RL "$source_dir/." "$checkout/spikes/authz/"

git -C "$checkout" init -q
git -C "$checkout" config user.email authz-bazel-test@example.invalid
git -C "$checkout" config user.name authz-bazel-test
git -C "$checkout" add -A .
git -C "$checkout" commit -qm acceptance

cd "$checkout/spikes/authz"
sha256sum go.mod go.sum >"$output_dir/source-before.sha256"
verify_module_inputs() {
  local status=$?
  trap - EXIT
  sha256sum go.mod go.sum >"$output_dir/source-after.sha256"
  if ! cmp "$output_dir/source-before.sha256" "$output_dir/source-after.sha256"; then
    echo 'contract acceptance changed go.mod or go.sum' >&2
    exit 1
  fi
  echo 'unchanged go.mod and go.sum verified after all harness executions'
  exit "$status"
}
trap verify_module_inputs EXIT

# These controls invoke the whole harness. An anticipated runner failure
# inside an otherwise passing harness is not enough to prove test validity.
diagnostics="$TEST_TMPDIR/diagnostics"
mkdir "$diagnostics"
cat >"$diagnostics/selected-go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
test "$#" = 3 && test "$1" = run && test "$2" = ./cmd/authz-spike && test "$3" = -print-contract
printf '%s\n' "$*" >>"${AUTHZ_DIAGNOSTIC_SDK_LOG:?}"
case "${AUTHZ_DIAGNOSTIC_MODE:?}" in
  unavailable) exit 95 ;;
  malformed) printf '%s\n' 'invalid exporter JSON' ;;
  *) exit 97 ;;
esac
GO
chmod +x "$diagnostics/selected-go"

expect_harness_failure() {
  local name="$1" selected_sdk="$2" selected_proxy="$3" reason="$4"
  local caches="$diagnostics/$name-caches" status=0
  local log="$output_dir/$name.log"
  mkdir -p "$caches/modules" "$caches/build" "$caches/path"
  test -z "$(find "$caches" -mindepth 2 -print -quit)"
  GOMODCACHE="$caches/modules" GOCACHE="$caches/build" GOPATH="$caches/path" \
    GOPROXY="file://$selected_proxy" AUTHZ_TEST_REAL_GO="$selected_sdk" \
    ./manifest-contract-test.sh >"$log" 2>&1 || status=$?
  if [[ "$status" == 0 ]] || ! grep -Fq "$reason" "$log" \
    || grep -Eq 'rejected before setup|valid contract reached fake|real executable contract export succeeded' "$log"; then
    echo "invalid harness failure control (exit $status): $name" >&2
    cat "$log" >&2
    exit 1
  fi
  printf '%s\t%s\n' "$name" "$status" >>"$output_dir/harness-failure-controls.tsv"
  echo "whole harness failed before any mutation success (exit $status): $name"
}

for mode in unavailable malformed; do
  reason='initial real executable export failed'
  if [[ "$mode" == malformed ]]; then
    reason='initial real executable export returned invalid JSON'
  fi
  sdk_log="$output_dir/selected-sdk-$mode-args.log"
  AUTHZ_DIAGNOSTIC_MODE="$mode" AUTHZ_DIAGNOSTIC_SDK_LOG="$sdk_log" \
    expect_harness_failure "selected-sdk-$mode" "$diagnostics/selected-go" "$proxy_dir" "$reason"
  test "$(cat "$sdk_log")" = 'run ./cmd/authz-spike -print-contract'
done
expect_harness_failure selected-sdk-missing "$diagnostics/missing-go" "$proxy_dir" \
  'executable Go SDK not found'

# Use symlinks to the declared files; only the disposable copies lose/change
# a required archive. Both diagnostics get empty caches and the real SDK.
python3 - "$proxy_dir" "$diagnostics" <<'PY'
from pathlib import Path
import sys

source, output = map(Path, sys.argv[1:])
required = Path("github.com/cedar-policy/cedar-go/@v/v1.8.0.zip")
assert (source / required).is_file()
for name in ("missing", "corrupt"):
    proxy = output / (name + "-proxy")
    for member in source.rglob("*"):
        if member.is_file():
            target = proxy / member.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(member.resolve())
    (proxy / required).unlink()
    if name == "corrupt":
        (proxy / required).write_bytes(b"deliberately corrupt required module archive\n")
PY
expect_harness_failure required-proxy-member-missing "$go_bin" "$diagnostics/missing-proxy" \
  'no such file or directory'
expect_harness_failure required-proxy-member-corrupt "$go_bin" "$diagnostics/corrupt-proxy" \
  'zip: not a valid zip file'

test -z "$(find "$TEST_TMPDIR/main-caches" -mindepth 2 -print -quit)"
echo 'starting authentic acceptance with empty private Go caches and declared file proxy'
AUTHZ_TEST_REAL_GO="$go_bin" AUTHZ_TEST_EVIDENCE_DIR="$output_dir" ./manifest-contract-test.sh
