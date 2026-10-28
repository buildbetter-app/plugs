#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${UX_SPIKE_PORT:-4176}"
TMP="$(mktemp -d -t plug-ux-check.XXXXXX)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$ROOT/spikes/ux" >"$TMP/server.log" 2>&1 &
SERVER_PID=$!
for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null

CHROME="${CHROME_PATH:-/usr/bin/google-chrome}"
for budget in 25 50 100; do
  for view in home next blocked changes people epic catalog; do
    dom="$TMP/${view}-${budget}.html"
    "$CHROME" --headless --disable-gpu --no-sandbox --dump-dom \
      "http://127.0.0.1:${PORT}/?budget=${budget}&view=${view}" >"$dom" 2>/dev/null
    grep -q "${budget} shown\|snapshot.*${budget}" "$dom"
    ! grep -qE "(undefined|NaN|javascript error)" "$dom"
    case "$view" in
      home) class="bead-title" ;;
      next) class="queue-row" ;;
      blocked) class="blocked-row" ;;
      changes) class="timeline-row" ;;
      people) class="person-detail-row" ;;
      epic) class="epic-row" ;;
      catalog) class="catalog-spec" ;;
    esac
    count="$(grep -o "class=\"${class}\"" "$dom" | wc -l | tr -d ' ')"
    if [[ "$view" == changes ]]; then
      (( count <= 16 ))
    elif [[ "$view" == people ]]; then
      (( count == 5 ))
    elif [[ "$view" == epic ]]; then
      (( count == 8 ))
    elif [[ "$view" == catalog ]]; then
      (( count == 3 ))
    else
      (( count <= budget ))
    fi
  done
done

echo "UX spike browser checks passed for budgets 25, 50, and 100"
