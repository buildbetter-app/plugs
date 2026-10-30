#!/usr/bin/env bash
# Run with a binary built beforehand; this script never starts a build.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/demo.sh /path/to/plug (build //cmd/plug:plug first)" >&2
  exit 2
fi
command -v jq >/dev/null || { echo "The demo requires jq." >&2; exit 2; }
demo_binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
[[ -x "$demo_binary" ]] || { echo "Plug binary is not executable: $demo_binary" >&2; exit 2; }
demo_dir=$(mktemp -d "${TMPDIR:-/tmp}/plug-demo.XXXXXX")
demo_pid=
demo_endpoint=
# The disposable server uses the existing anonymous loopback policy.
unset PLUG_TOKEN

stop_server() {
  local stopped_pid=$demo_pid
  demo_pid=
  kill -TERM "$stopped_pid" 2>/dev/null || true
  wait "$stopped_pid"
}
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "$demo_pid" ]]; then stop_server || status=1; fi
  if [[ $status -ne 0 && -f "$demo_dir/server.log" ]]; then cat "$demo_dir/server.log" >&2; fi
  rm -rf -- "$demo_dir"
  printf 'Cleanup: server stopped; removed %s\n' "$demo_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

mkdir "$demo_dir/.plug"
cat > "$demo_dir/.plug/config.yaml" <<'CONFIG'
driver: sqlite
dsn: demo.db
listen: 127.0.0.1:0
project: demo
ui: false
CONFIG

start_server() {
  demo_endpoint=
  printf '+ plug server (working directory %s)\n' "$demo_dir"
  (cd "$demo_dir"; exec "$demo_binary" server) > "$demo_dir/server.log" 2>&1 &
  demo_pid=$!
  for ((attempt=0; attempt<100; attempt++)); do
    kill -0 "$demo_pid" 2>/dev/null || { echo "Server exited during startup." >&2; return 1; }
    demo_endpoint=$(sed -n 's/^plug server listening on //p' "$demo_dir/server.log")
    if [[ -n "$demo_endpoint" ]]; then
      printf 'Server: %s\n' "$demo_endpoint"
      return
    fi
    sleep 0.05
  done
  echo "Server did not report a listener within five seconds." >&2
  return 1
}
cli() {
  printf '+ plug --server %q --project demo --agent quickstart --json' "$demo_endpoint" >&2
  printf ' %q' "$@" >&2
  printf '\n' >&2
  "$demo_binary" --server "$demo_endpoint" --project demo --agent quickstart --json "$@"
}
create() {
  local result
  result=$(cli create --title "$1")
  printf '%s\n' "$result" >&2
  jq -er '.results[0].ref.id' <<< "$result"
}

start_server
demo_blocker=$(create "Prepare the demo")
demo_work=$(create "Deliver the demo")
cli link "$demo_work" "$demo_blocker" --kind blocks
demo_ready=$(cli ready --limit 10)
printf '%s\n' "$demo_ready"
jq -e --arg blocker "$demo_blocker" --arg work "$demo_work" \
  'any(.items[]; .id == $blocker) and all(.items[]; .id != $work)' <<< "$demo_ready" >/dev/null

cli claim "$demo_blocker"
cli update "$demo_blocker" --title "Demo prepared"
cli close "$demo_blocker" --reason "Preparation complete"
demo_ready=$(cli ready --limit 10)
printf '%s\n' "$demo_ready"
jq -e --arg work "$demo_work" 'any(.items[]; .id == $work)' <<< "$demo_ready" >/dev/null
cli claim "$demo_work"
cli update "$demo_work" --title "Demo delivered"
cli close "$demo_work" --reason "Delivery complete"
cli get "$demo_work" | tee "$demo_dir/before.json"

stop_server
echo 'Restarting the server with the same SQLite file.'
start_server
cli get "$demo_work" | tee "$demo_dir/after.json"
jq -e '.bead.fields.title == "Demo delivered" and .bead.fields.status == "done"' "$demo_dir/after.json" >/dev/null
cmp <(jq -S '.bead | {id, type, project, fields, revision}' "$demo_dir/before.json") \
    <(jq -S '.bead | {id, type, project, fields, revision}' "$demo_dir/after.json")
echo 'PASS: dependency readiness, remote writes, and SQLite restart readback.'
