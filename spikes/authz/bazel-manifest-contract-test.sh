#!/usr/bin/env bash

# Bazel acceptance wrapper for the nested authz Go module. The runner records
# git-relative source digests, so lay the declared runfiles out as a tiny
# throwaway repository before invoking the same contract harness used by
# contributors. The harness replaces every benchmark invocation with a
# guarded fake command; only -print-contract is executed by the real module.

set -euo pipefail

go_runfile="${1:?expected the registered Go SDK runfile path}"
runfiles="${TEST_SRCDIR:?}/$TEST_WORKSPACE"
source_dir="$runfiles/spikes/authz"
checkout="$TEST_TMPDIR/authz-checkout"

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

go_bin="$TEST_SRCDIR/$go_runfile"
test -x "$go_bin"

cd "$checkout/spikes/authz"
AUTHZ_TEST_REAL_GO="$go_bin" ./manifest-contract-test.sh
