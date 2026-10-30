# B28 recorded acceptance slice

This package covers four dependency-ready B28 cases through Plug's real
pipeline, Writer, service, byte JSON-RPC, and file-backed SQLite driver:

1. a mutator and validator run before commit, while a post subscriber runs
   after commit and reads the committed revision;
2. a fail-closed pre-subscriber refusal publishes no record, head, or post
   callback;
3. a post failure preserves the committed result as a deterministic
   `post_subscriber_failed` warning and replay does not invoke the callback;
4. a same-value no-op and its idempotency replay retain the original row
   revision and do not invoke post delivery.

`record_reference.py` records the selected non-temporal Beads hook behavior
from pinned source commit `9d6fff760b7a314b476539198504d77ce0959656` in two
independently initialized workspaces. The retained recordings bind the source
tests/files, executable digest, command-output digests, and stable operation
projection. They preserve the reference's post-commit hook ordering and
refusal suppression without treating timestamps or the script envelope as
Plug expectations.

Plug intentionally adopts typed pre/post subscribers and committed-result
warnings instead of Beads' script-hook envelope. This slice does not claim
dead-letter storage, breaker/alert integration, every operation mapping,
cross-process callback recovery, temporal behavior, or full B28 parity.

Run the bounded target with:

```sh
bazel --ignore_all_rc_files test //spec/cardb28:acceptance --test_output=all
```
