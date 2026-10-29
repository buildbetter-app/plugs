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

write_manifest() {
  local tmp_manifest
  tmp_manifest="$(mktemp "${TMPDIR:-/tmp}/plug-bazel-manifest.XXXXXX")" || return 1
  if ! {
    find "$evidence_dir" -type f ! -path "$manifest_path" -print0 \
      | LC_ALL=C sort -z \
      | xargs -r -0 /usr/bin/sha256sum
    if [[ -d bazel-testlogs ]]; then
      find -L bazel-testlogs -type f -name 'test.xml' -print0 \
        | LC_ALL=C sort -z \
        | xargs -r -0 /usr/bin/sha256sum
    fi
  } >"$tmp_manifest"; then
    rm -f "$tmp_manifest"
    return 1
  fi
  mv -f "$tmp_manifest" "$manifest_path"
}

finish() {
  local status=$?
  printf 'exit_status=%s\nphase=%s\n' "$status" "$phase" \
    >"$evidence_dir/preflight-result.txt"
  if ! write_manifest; then
    echo "plug-bazel-preflight: could not write $manifest_path" >&2
    ((status == 0)) && status=1
  fi
  exit "$status"
}
trap finish EXIT
# Preserve a signal status in the evidence when the runner or an outer timeout
# stops the preflight while Bazel is running.  The EXIT trap still writes the
# result and manifest before returning the conventional shell signal status.
trap 'exit 143' TERM
trap 'exit 130' INT

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
  --nosystem_rc
  --noworkspace_rc
  "--bazelrc=$rc"
  "--output_user_root=$output_root"
)

run_bazel() {
  "$bazelisk" "${startup[@]}" "$@"
}

version_output="$(BAZELISK_HOME="$BAZELISK_HOME" BAZELISK_VERIFY_SHA256="$BAZELISK_VERIFY_SHA256" "$bazelisk" --version 2>&1)"
printf '%s\n' "$version_output" | tee "$evidence_dir/bazel-version.txt"
grep -Fq "bazel ${bazel_version}" <<<"$version_output" || fail "Bazelisk did not resolve Bazel $bazel_version"

release_output="$(run_bazel info release 2>&1)"
printf '%s\n' "$release_output" | tee "$evidence_dir/bazel-release.txt"
grep -Fxq "release $bazel_version" <<<"$release_output" || fail "Bazel release is not $bazel_version"

go_sdk_output="$(run_bazel cquery @go_sdk//:go_sdk --output=build 2>&1)"
printf '%s\n' "$go_sdk_output" >"$evidence_dir/go-sdk.txt"
go_sdk_version="$(sed -n 's/^  version = "\([^"]*\)".*/\1/p' "$evidence_dir/go-sdk.txt" | head -1)"
[[ -n "$go_sdk_version" ]] || fail "could not resolve the Bazel Go SDK version"

if command -v nix >/dev/null 2>&1; then
  nix_version="$(nix --version 2>&1)"
else
  nix_version="unavailable on runner"
fi

source_digest="$(git ls-files -z | xargs -0 /usr/bin/sha256sum | LC_ALL=C sort -k2 | /usr/bin/sha256sum | awk '{print $1}')"
generator_inputs=(.bazelversion MODULE.bazel MODULE.bazel.lock)
for generator_input in go.mod go.sum gazelle.go.work tools/nogo/go.mod tools/nogo/go.sum; do
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

build_bep="$evidence_dir/build-events.json"
test_bep="$evidence_dir/test-events.json"
build_command=("${startup[@]}" build "${targets[@]}" "--build_event_json_file=$build_bep" --show_result=0)
test_command=("${startup[@]}" test "${targets[@]}" "--build_event_json_file=$test_bep" --nocache_test_results --test_output=errors)

printf '%q ' "$bazelisk" "${build_command[@]}" | sed 's/ $//' >"$evidence_dir/build-command.txt"
printf '\n' >>"$evidence_dir/build-command.txt"
printf '%q ' "$bazelisk" "${test_command[@]}" | sed 's/ $//' >"$evidence_dir/test-command.txt"
printf '\n' >>"$evidence_dir/test-command.txt"

phase=build
echo "plug-bazel-preflight: bazel build $targets_text"
run_bazel build "${targets[@]}" "--build_event_json_file=$build_bep" --show_result=0 2>&1 | tee "$evidence_dir/build.log"

phase="test"
echo "plug-bazel-preflight: bazel test $targets_text (fresh results)"
run_bazel test "${targets[@]}" "--build_event_json_file=$test_bep" --nocache_test_results --test_output=errors 2>&1 | tee "$evidence_dir/test.log"

phase=complete
echo "plug-bazel-preflight: evidence=$evidence_dir"
echo "plug-bazel-preflight: bazel=$bazel_version go_sdk=$go_sdk_version runner=$runner_class"
