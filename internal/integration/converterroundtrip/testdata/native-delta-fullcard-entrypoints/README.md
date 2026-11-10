# Current public CP11 receiving inputs

These three archives are exact original producer outputs from
`53d909138a21fe34b3915e6aca66c7ccaf635a9b`, public run `42864951650`.
They were copied without rewriting any archive member from the original
`safe_diagnostics_test` outputs. The stores were constructed public fixtures;
they contain no private host data. Their native captures are independent of
the later retained native delta producer/receiver pair.

| Original | SHA256 | Bytes | Verified images | History |
| --- | --- | --- | --- | --- |
| public-cp11-loose-missing-root.tar | d381b6b254947f9d466e0d1354e9c7b05263308aca6c0ad30f217638cbfd8ec0 | 293888 | 12 | verified to missing_root_chunk boundary, ordinal 13 |
| public-cp11-darc-missing-root.tar | b1f7f3f5d5d7f3132a0eff8a22d414957e43eaf699e45515b356686ea6be9ba7 | 293888 | 12 | verified to missing_root_chunk boundary, ordinal 13 |
| public-cp11-darc-complete.tar | 9d2012775a493a5281ae72380e0376aa8bb16818ab28fa9b346252153ea67fa5 | 312320 | 14 | complete |

Each original uses project `public`, schema cursors 66/26, and 30 portable
current families with 110 rows. Each missing-root original retains 247
historical tables and 881 rows, plus its exact two-event attachment. The
complete original retains 266 historical tables and 942 rows. The original
source identities stay distinct and are checked in the callable test adapter.

`TestNativeFullCardLibraryReport` accepts these fixed inputs through the existing
library import and recovery path across memory, SQLite and PostgreSQL. It emits
`public-<case>-library-<engine>.json` with the recovered committed report and
preserves independent current/history cell readback, source-blob equality,
populated-target refusal, and persistent close/reopen checks.
`TestNativeFullCardDefaultCLI` runs the three PostgreSQL constructor/default
receiving positives through the built receiver. PostgreSQL is the existing
normal CI public fixture; no external DSN is required.

The focused remote Bazel selection is
`--test_filter=^TestNativeFullCard(LibraryReport|DefaultCLI)$` on
`//internal/integration/converterroundtrip:converterroundtrip_test`, with
`--test_env=GO_TEST_WRAP_TESTV=1` for literal named RUN/PASS output. A separate
unfiltered whole `//...` original and independent review remain required.
These twelve positives confer no private, retained-pair, public1200, procedure,
live switch or FullS1 qualification.
