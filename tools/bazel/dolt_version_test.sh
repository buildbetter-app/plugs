#!/usr/bin/env bash
# Owns its pinned binary through env/data; independent of any run_under.
# Fails unless that exact hermetic binary reports DOLT_VERSION.
set -euo pipefail

bzl="${TEST_SRCDIR}/${TEST_WORKSPACE}/tools/bazel/dolt.bzl"
want="$(sed -n 's/^DOLT_VERSION = "\([0-9.]*\)"$/\1/p' "$bzl")"
if [[ -z "$want" ]]; then
	echo "dolt_version_test: DOLT_VERSION not found in $bzl" >&2
	exit 1
fi
if [[ -z "${BEADS_TEST_DOLT_BINARY:-}" ]]; then
	echo "dolt_version_test: BEADS_TEST_DOLT_BINARY unset (not run under //tools/bazel:test_env?)" >&2
	exit 1
fi
binary="$BEADS_TEST_DOLT_BINARY"
if [[ "$binary" != /* ]]; then
	binary="${TEST_SRCDIR}/${binary}"
fi
got="$("$binary" version | sed -n 's/^dolt version \([0-9.]*\).*/\1/p')"
if [[ "$got" != "$want" ]]; then
	echo "dolt_version_test: hermetic dolt reports '$got', tools/bazel/dolt.bzl pins '$want'" >&2
	exit 1
fi
echo "hermetic dolt $got matches DOLT_VERSION"
