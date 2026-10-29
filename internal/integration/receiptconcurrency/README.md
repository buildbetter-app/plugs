# Receipt concurrency acceptance

This target drives the production service and Writer against memory, a real
file-backed SQLite store, and a disposable PostgreSQL 17 store. Two calls with
one actor, method, key, and request body are held at the existing ontology
normalization boundary and released together. The assertions require exactly
one durable commit, one replay, one record/version/event effect, and the same
receipt after a runtime/store reopen. They also check that actor and method are
part of the receipt scope and that a changed body under the original scope is
refused without changing any durable state.

Memory has no persistence boundary, so its reopen step releases and reacquires
the lease and reconstructs the service/Writer over the same in-process store.
SQLite and PostgreSQL close and reopen their actual durable stores. The test
does not cover transport cancellation, API-key admission, crash recovery,
performance, failover, or production topology.

```sh
bazel --ignore_all_rc_files test //internal/integration/receiptconcurrency:receiptconcurrency_test --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
```
