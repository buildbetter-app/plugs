# UX spike: low-noise views

This is a throwaway, dependency-free browser prototype for A7. It uses a deterministic in-browser fixture adapter to model a 20,000-bead corpus and 50 fixture agents while rendering only the selected 25, 50, or 100 node budget. The pinned `bd serve` probe is separate: this page has no live server or watch connection.

The prototype covers five questions:

- **What next** — dependency-aware queue rows with an explanation for each suggestion.
- **Why blocked** — blocker references and a short reason, with an aggregate for causes outside the budget.
- **What changed** — a coalesced delta list that keeps row order stable.
- **Who is on what** — five visible collaborators with claims, focus, and load.
- **Is the epic on track** — bounded progress roll-ups rather than a graph wall.

It also includes a small D31 declarative catalog for a panel, column, and button. No CDN, build step, or server-side code is needed.

## Run locally

From the repository root:

```sh
python3 -m http.server 4175 --directory spikes/ux
```

Open <http://127.0.0.1:4175/>. Use the node-budget control and the view tabs to compare the information density at 25, 50, and 100 visible nodes. The page is intentionally static so it can be captured by headless Chrome:

```sh
/usr/bin/google-chrome --headless --disable-gpu --no-sandbox \
  --window-size=1440,1000 --screenshot=/tmp/plug-ux-home.png \
  http://127.0.0.1:4175/
```

For the decision record's comparison matrix, run `./spikes/ux/capture.sh`. It
writes screenshots for Home, What next, Why blocked, What changed, People, and
Epic health at all three budgets into `spec/ux-screens/` (or the output
directory supplied as its first argument).

The browser smoke check runs the same matrix through headless Chrome, checks
that each view stays within its node budget, binds every rendered bead row to
the independent `fixture-oracle.py`, and fails on browser `console.error` or an
uncaught exception through `browser-check.mjs`:

```sh
./spikes/ux/check.sh
```

The fixture and its relations are in `app.js` (`makeCorpus`); replacing that adapter with `bd serve` `view.run`/`view.watch` requests is the intended next step after the view decisions are made. Labels in the page call out fixture, server, and watch boundaries explicitly.
