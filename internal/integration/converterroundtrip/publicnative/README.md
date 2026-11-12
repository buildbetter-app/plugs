# Public native collection proof

This integration helper uses the landed capture-set reader and existing
collection issuer. Positive joins run the actual `native-receive` constructor
and row receiver. It adds no mapping or storage implementation.

`inputs.json` pins six public inputs by source identity, commit count and each
original file's name, length and SHA-256. The small ops/sv2 pair has 97 commits
per source and is already committed with its creation record. The external
ops/sv2 pair has 1,614 commits per source. Each external set exceeds 20 MiB and
stays outside the repository. The two external 1,200-commit inputs share one
project/history and have different store identities; they are a collision
negative, never a distinct-project positive.

The remote `publicnative_test` authenticates both small sources, corrupts a
separate staged part to check refusal cleanup, checks input order and pins,
and executes the Python runner against the actual small memory receiver.
An explicit cancellation probe checks that the outer test signals the runner,
which terminates and waits for its active child group and retains that wait.
Regular temporary copies accommodate Bazel runfiles without weakening the
reader's directory or symlink guards. Test outputs retain both exact remotely
built executables, this script, the policy, input registry and command
originals. Runtime verdicts require their terminal test evidence.

After the separate scale accounting hold is cleared, use those exact retained
executables with `run-public-proof.py`. It requires their SHA-256 values and a
fresh output directory, records argv, stdout, stderr and child wait status,
and applies one 7,200-second deadline. It neither builds nor captures data.

The cases are:

- `--case source --source NAME --directory SET`: authenticate one pinned input,
  check its complete original-file ledger and commit census, then discard the
  archive and detached ledger. Both live bytes and reservation counts must be
  zero. Run separately for each public 1,200 and 1,614 source.
- `--case collision --source public1200-loose --source public1200-archived
  --directory LOOSE --directory ARCHIVED`: use the real history-inclusive
  issuer, require the typed `cross_project_id_collision` with a nonzero count,
  and close every owner. There is no destination store or publication call.
- `--case positive --source scale-ops --source scale-sv2 --directory OPS
  --directory SV2 --receiver-binary RECEIVER --receiver-sha256 SHA
  --policy POLICY --driver ENGINE`: build only the pinned input descriptions,
  then run the existing constructor and receiver. The independent Python
  oracle checks the ordered commitment/key, one authority/provider, both
  source histories, total head, imported and reopened byte readbacks, and
  observed per-source preparation counts. SQL engines also need `--target`.

Every case requires `--proof-binary PROOF --proof-sha256 SHA --output FRESH`.
Start processes with the swarm's worker-safe PATH. A `PASS.json` describes only
the case that actually ran. Successful reader call counts are not full
per-original byte/EOF/close ledgers; those remain the separate receiving tests.
Decoded reservations and process RSS are separate measurements. Source pins,
metadata construction, a refusal or a small-pair pass do not qualify the scale
join, private rehearsal, current main, landing or FullS1. This script grants no
runtime or private authorization.
