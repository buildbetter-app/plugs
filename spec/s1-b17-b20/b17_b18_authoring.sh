#!/usr/bin/env bash
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true && "${GITHUB_REPOSITORY:-}" == buildbetter-app/plugs ]]
cd "${BUILD_WORKSPACE_DIRECTORY:?missing RunsOn source root}"
printf 'AUTHORING_SOURCE %s %s\n' "$(git rev-parse HEAD)" "$(git rev-parse HEAD^{tree})"
[[ -z "$(git status --porcelain=v1)" ]]
# Format only this card's new/changed Go inputs, never a shared checkout sweep.
gofmt -w internal/core/service/workflow_metadata.go internal/core/service/source_workflow.go \
  internal/server/authzrpc/history.go internal/server/rpc/service.go \
  spec/s1-b17-b20/b17_precision_test.go spec/s1-b17-b20/b17_ephemeral_test.go \
  spec/s1-b17-b20/b18_current_permission_test.go spec/cardb17b18/native_test.go
python3 scripts/ci/reconcile-openrpc.py --root "$PWD" --write
go run -tags gms_pure_go ./internal/server/rpc/cmd/openrpcgen -out internal/server/rpc/openrpc.json
python3 scripts/ci/reconcile-openrpc.py --root "$PWD"
python3 - <<'PY'
import base64,hashlib,io,json,pathlib,subprocess,tarfile
names=subprocess.check_output(['git','diff','--name-only','HEAD'],text=True).splitlines()
out=io.BytesIO()
with tarfile.open(fileobj=out,mode='w') as archive:
    for name in names:
        archive.add(name,arcname=name,recursive=False)
print('AUTHORING_FILES '+json.dumps([{'path':name,'sha256':hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest()} for name in names],sort_keys=True))
print('AUTHORING_TAR_BASE64_BEGIN')
print(base64.b64encode(out.getvalue()).decode())
print('AUTHORING_TAR_BASE64_END')
PY
