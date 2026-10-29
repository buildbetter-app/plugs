#!/usr/bin/env bash
# Run Plug's clean, local Bazel preflight.
#
# This script does not consume the repository .bazelrc. The current checkout
# still contains the Beads-era rc, including optional remote settings; the clean
# Plug lane must remain usable while that tree is being replaced. It reads only
# the inherited gms_pure_go declaration when go-mysql-server is still present,
# and refuses to infer that compatibility setting if its declaration is absent.
# A temporary rc and output root make every invocation independent of a user's
# Bazel server, disk cache, credentials, and workspace rc files.
set -euo pipefail

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

evidence_dir="${PLUG_BAZEL_EVIDENCE_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/plug-bazel-evidence.XXXXXX")}"
mkdir -p "$evidence_dir"

manifest_path="$evidence_dir/manifest.sha256"
phase=initializing
work_dir=
testlogs_root=
active_bazel_pid=
stream_pid=
termination_signal=none

# Each invocation uses batch mode in its own session: no Bazel daemon can keep
# writing after its client exits. Only signal processes descended from our PID.
stop_owned_bazel() {
  local pid state attempt signal running=false
  local -a owned_pids=()
  if [[ -n "$active_bazel_pid" ]]; then
    mapfile -t owned_pids < <(ps -eo pid=,ppid= | awk -v root="$active_bazel_pid" '
      { parent[$1] = $2 }
      END {
        owned[root] = 1
        do {
          changed = 0
          for (pid in parent)
            if ((parent[pid] in owned) && !(pid in owned)) {
              owned[pid] = 1
              changed = 1
            }
        } while (changed)
        for (pid in owned) print pid
      }')
    for pid in "${owned_pids[@]}"; do
      [[ "$pid" =~ ^[0-9]+$ ]] && ((pid > 1 && pid != $$ && pid != PPID)) || return 1
    done
    for signal in INT TERM KILL; do
      kill -s "$signal" -- "-$active_bazel_pid" 2>/dev/null || :
      # Give Bazel time to cancel actions itself before terminating descendants.
      if [[ "$signal" != INT ]]; then
        kill -s "$signal" -- "${owned_pids[@]}" 2>/dev/null || :
      fi
      for ((attempt = 0; attempt < 10; attempt++)); do
        running=false
        for pid in "${owned_pids[@]}"; do
          state="$(ps -o stat= -p "$pid")" || continue
          [[ "$state" == *Z* ]] || running=true
        done
        [[ "$running" == true ]] || break
        sleep 0.5
      done
      [[ "$running" == true ]] || break
    done
    wait "$active_bazel_pid" 2>/dev/null || :
    active_bazel_pid=
    if [[ "$running" == true ]]; then
      echo 'plug-bazel-preflight: owned Bazel processes did not stop; evidence cannot freeze' >&2
      return 1
    fi
  fi
  if [[ -n "$stream_pid" ]]; then
    kill "$stream_pid" 2>/dev/null || :
    wait "$stream_pid" 2>/dev/null || :
    stream_pid=
  fi
}

snapshot_evidence() {
  local name xml relative
  [[ -n "$work_dir" ]] || return 0
  for name in bazel-version.txt bazel-release.txt go-sdk.txt bazel-testlogs.txt \
    build-command.txt test-command.txt build-events.json test-events.json \
    build.log test.log owned-processes.txt plug.bazelrc; do
    if [[ -f "$work_dir/$name" ]]; then
      cp -- "$work_dir/$name" "$evidence_dir/$name" || return 1
    fi
  done
  # Read only our output root, never the mutable workspace bazel-testlogs link.
  if [[ -n "$testlogs_root" && -d "$testlogs_root" ]]; then
    while IFS= read -r -d '' xml; do
      relative="${xml#"$testlogs_root/"}"
      mkdir -p -- "$evidence_dir/test-xml/$(dirname "$relative")" || return 1
      cp -- "$xml" "$evidence_dir/test-xml/$relative" || return 1
    done < <(find -L "$testlogs_root" -type f -name test.xml -print0)
  fi
  df -B1 -- "$work_dir" "$evidence_dir" >"$evidence_dir/disk-after-preflight.txt"
}

write_manifest() {
  local tmp_manifest
  tmp_manifest="$(mktemp "${TMPDIR:-/tmp}/plug-bazel-manifest.XXXXXX")" || return 1
  if ! (
    cd "$evidence_dir" || exit 1
    find . -type f ! -path ./manifest.sha256 -print0 \
      | LC_ALL=C sort -z \
      | xargs -r -0 /usr/bin/sha256sum
  ) >"$tmp_manifest"; then
    rm -f "$tmp_manifest"
    return 1
  fi
  mv -f "$tmp_manifest" "$manifest_path"
}

finish() {
  local status=$?
  local evidence_frozen=true
  trap - EXIT
  trap '' INT TERM
  if ! stop_owned_bazel || ! snapshot_evidence; then
    evidence_frozen=false
    ((status != 0)) || status=1
  fi
  printf 'exit_status=%s\nphase=%s\ntermination_signal=%s\nevidence_frozen=%s\nbazel_mode=batch\n' \
    "$status" "$phase" "$termination_signal" "$evidence_frozen" \
    >"$evidence_dir/preflight-result.txt"
  if [[ "$evidence_frozen" == true ]] && ! write_manifest; then
    echo "plug-bazel-preflight: could not write $manifest_path" >&2
    ((status == 0)) && status=1
  fi
  exit "$status"
}
trap finish EXIT
# Preserve a signal status in the evidence when the runner or an outer timeout
# stops the preflight while Bazel is running.  The EXIT trap still writes the
# result and manifest before returning the conventional shell signal status.
trap 'termination_signal=TERM; exit 143' TERM
trap 'termination_signal=INT; exit 130' INT

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

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/plug-bazel-bootstrap.XXXXXX")"
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
  local log="$1" status
  shift
  # Reset the signals Bash normally ignores for asynchronous children. Keeping
  # the wait in this shell lets TERM/INT interrupt it and enter the EXIT trap.
  setsid --wait env --default-signal=INT,TERM \
    "$bazelisk" "${startup[@]}" "$@" >"$log" 2>&1 &
  active_bazel_pid=$!
  printf 'pid=%s command=%s\n' "$active_bazel_pid" "$1" >>"$work_dir/owned-processes.txt"
  tail --pid="$active_bazel_pid" --sleep-interval=0.1 -n +1 -f "$log" &
  stream_pid=$!
  if wait "$active_bazel_pid"; then status=0; else status=$?; fi
  active_bazel_pid=
  wait "$stream_pid" || :
  stream_pid=
  return "$status"
}

run_bazel "$work_dir/bazel-version.txt" version --gnu_format
grep -Fq "bazel ${bazel_version}" "$work_dir/bazel-version.txt" || fail "Bazelisk did not resolve Bazel $bazel_version"

run_bazel "$work_dir/bazel-release.txt" info release
grep -Fxq "release $bazel_version" "$work_dir/bazel-release.txt" || fail "Bazel release is not $bazel_version"

run_bazel "$work_dir/bazel-testlogs.txt" info bazel-testlogs
testlogs_root="$(tail -1 "$work_dir/bazel-testlogs.txt")"
[[ "$testlogs_root" == "$output_root/"* ]] || fail "Bazel test logs escaped the owned output root"

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
echo "plug-bazel-preflight: bazel build $targets_text"
run_bazel "$work_dir/build.log" build "${targets[@]}" "--build_event_json_file=$build_bep" --show_result=0

phase="test"
echo "plug-bazel-preflight: bazel test $targets_text (fresh results)"
run_bazel "$work_dir/test.log" test "${targets[@]}" "--build_event_json_file=$test_bep" --nocache_test_results --test_output=errors

phase=complete
echo "plug-bazel-preflight: evidence=$evidence_dir"
echo "plug-bazel-preflight: bazel=$bazel_version go_sdk=$go_sdk_version runner=$runner_class"
