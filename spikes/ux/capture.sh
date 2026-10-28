#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-$ROOT/spec/ux-screens}"
PORT="${UX_SPIKE_PORT:-4175}"
SERVER_LOG="$(mktemp -t plug-ux-server.XXXXXX.log)"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$SERVER_LOG"
}
trap cleanup EXIT

mkdir -p "$OUT"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$ROOT/spikes/ux" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null

CHROME="${CHROME_PATH:-/usr/bin/google-chrome}"
for budget in 25 50 100; do
  for view in home next blocked changes people epic; do
    target="$OUT/${view}-${budget}.png"
    "$CHROME" --headless --disable-gpu --no-sandbox --hide-scrollbars \
      --window-size=1440,1200 --screenshot="$target" \
      "http://127.0.0.1:${PORT}/?budget=${budget}&view=${view}" \
      >/dev/null 2>&1
    test -s "$target"
  done
done

"$CHROME" --headless --disable-gpu --no-sandbox --hide-scrollbars \
  --window-size=1440,1200 --screenshot="$OUT/catalog.png" \
  "http://127.0.0.1:${PORT}/?budget=25&view=catalog" >/dev/null 2>&1
test -s "$OUT/catalog.png"

echo "captured UX spike screenshots in $OUT"
