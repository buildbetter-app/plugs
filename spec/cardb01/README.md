# B01 recorded acceptance slice

This package binds three bounded cases from behavior card B01 without claiming
the whole card:

1. two byte-identical, fresh embedded-Dolt recordings from pinned Beads commit
   `9d6fff760b7a314b476539198504d77ce0959656` retain open membership,
   closed/deferred/pinned exclusion, explicit priority order, and an equal-time
   ID tie;
2. real Plug SQLite plus byte JSON-RPC includes the default `todo` unstarted
   category and excludes lifecycle-projected `backlog` and completed `done`;
3. Plug's signed live ready cursor preserves priority/ID order and returns no
   duplicate in the selected monotonic-tail case: a lower-ranked row is written
   after page one, so the delivered prefix is not reordered.

`record_reference.py` created each retained JSON file in an independently
initialized disposable Git/Beads workspace. The recorder verifies the source
commit and tree, requires that pinned source tree to be clean, retains the
exact executable and version digests plus its documented pure-Go build tag,
imports fixed timestamps and IDs, invokes the ready command with explicit
priority ordering and no result limit, and refuses an unexpected projection.
The two files have SHA-256
`088e187096cd33de2fa83f656f0b0bc279f3b66c9bd1f31da04613c060c8bbc4`.

Reference and Plug status names intentionally differ under the adopted A3/D3
category design: this slice compares ready membership, not literal status
spelling. The reference uses explicit priority sort with equal timestamps for
the ID tie. Plug exercises its current priority/ID projection separately.

This package does **not** measure or certify P74's one-minute stored-order lag,
the hybrid 48-hour bucket, arbitrary live-reorder duplicate/skip behavior,
complete filters, parent propagation, dependencies, wisps, authorization,
performance, or all B01 cases. The ready cursor is live rather than a snapshot;
the design permits skipped or repeated rows when an intervening write reorders
the delivered prefix.

Run the bounded target with:

```sh
bazel --ignore_all_rc_files test //spec/cardb01:cardb01_test --test_output=all
```
