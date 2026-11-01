#!/usr/bin/env bash
set -euo pipefail

# This wrapper only dispatches source-bound synthetic tests on RunsOn.
if [[ $# == 0 || ( $# == 1 && $1 == --plan ) ]]; then
  printf '%s\n' 'PLAN: --run FULL_COMMIT dispatches three disposable preparations per memory/SQLite driver on RunsOn. This does not satisfy three consecutive nights or the shadow fortnight. No staging, managed cloud, switch, release or deployment is performed.'
  exit 0
fi
if [[ $# != 2 || $1 != --run || ! $2 =~ ^[0-9a-f]{40}$ ]]; then
  printf '%s\n' 'usage: three_rehearsals.sh [--plan | --run FULL_COMMIT]' >&2
  exit 2
fi
rehearsal_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
rehearsal_repo=$(git -C "$rehearsal_dir" rev-parse --show-toplevel)
rehearsal_sha=$(git -C "$rehearsal_repo" rev-parse --verify "$2^{commit}")
[[ $rehearsal_sha == "$2" ]]
rehearsal_tree=$(git -C "$rehearsal_repo" rev-parse --verify "$rehearsal_sha^{tree}")
exec /tmp/plug-swarm/bin/remote-bazel "$rehearsal_repo" "$rehearsal_sha" i8-three-preparations -- test \
  //tools/rehearsal:rehearsal_test //internal/cli:cli_test //tools/lexicon:lexicon_gate \
  --test_output=all --cache_test_results=no \
  "--test_env=R1_SOURCE_SHA=$rehearsal_sha" "--test_env=R1_SOURCE_TREE=$rehearsal_tree"
