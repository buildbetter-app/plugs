#!/usr/bin/env bash
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
set -euo pipefail
# RunsOn only. Execute via remote-bazel run; never compile on the shared host.
[[ "${GITHUB_ACTIONS:-}" == true && "${GITHUB_REPOSITORY:-}" == buildbetter-app/plugs ]]
cd "${BUILD_WORKSPACE_DIRECTORY:?missing remote workspace}"
source_sha=$(git rev-parse HEAD)
source_tree=$(git rev-parse HEAD^{tree})
source_parents=$(git cat-file -p HEAD | sed -n 's/^parent //p')
source_status=$(git status --porcelain=v1)
printf 'SOURCE_PRE sha=%s tree=%s parents=%s\n' "$source_sha" "$source_tree" "$source_parents"
printf 'SOURCE_PRE_STATUS_BEGIN\n%s\nSOURCE_PRE_STATUS_END\n' "$source_status"
[[ -z "$source_status" ]]
go version
set +e
printf 'COMPILE_COMMAND go build -tags gms_pure_go ./...\n'
go build -tags gms_pure_go ./...
build_rc=$?
printf 'COMPILE_EXIT %s\n' "$build_rc"
printf 'VET_COMMAND go vet -tags gms_pure_go ./...\n'
go vet -tags gms_pure_go ./...
vet_rc=$?
printf 'VET_EXIT %s\n' "$vet_rc"
set -e
post_sha=$(git rev-parse HEAD)
post_tree=$(git rev-parse HEAD^{tree})
post_status=$(git status --porcelain=v1)
printf 'SOURCE_POST sha=%s tree=%s\n' "$post_sha" "$post_tree"
printf 'SOURCE_POST_STATUS_BEGIN\n%s\nSOURCE_POST_STATUS_END\n' "$post_status"
[[ "$source_sha" == "$post_sha" && "$source_tree" == "$post_tree" && -z "$post_status" ]]
[[ "$build_rc" == 0 && "$vet_rc" == 0 ]]
