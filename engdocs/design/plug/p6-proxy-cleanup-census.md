# P6 Beads proxy cleanup census

All tracked external/native sites use the centralized StartDolt cleanup helper (`internal/publicnative/server_linux.go:243`), which scans the real DataRoot. Existing direct registrations remain explicit where present.

| Site | Evidence |
|---|---|
| `internal/integration/b03catalog/native_catalog_recording_test.go` | covered via centralized StartDolt helper (line 273); external=608, StartDolt=353 |
| `internal/integration/b03publiccatalog/native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=172, StartDolt=154 |
| `internal/integration/b04claimidentity/native_b04_recording_test.go` | covered via centralized StartDolt helper (line 273); external=192, StartDolt=169 |
| `spec/cardb01/canonicalfront/native_test.go` | covered via centralized StartDolt helper (line 273); external=246, StartDolt=384 |
| `spec/cardb07/native_test.go` | covered via centralized StartDolt helper (line 273); external=190, StartDolt=179 |
| `spec/cardb07effects/native_test.go` | covered via centralized StartDolt helper (line 273); external=124, StartDolt=117 |
| `spec/cardb08/native_test.go` | covered via centralized StartDolt helper (line 273); external=272, StartDolt=257 |
| `spec/cardb11/native_test.go` | covered via centralized StartDolt helper (line 273); external=189, StartDolt=178 |
| `spec/cardb16/native_reference_test.go` | covered via centralized StartDolt helper (line 273); external=92, StartDolt=87 |
| `spec/cardb17b18/native_test.go` | covered via centralized StartDolt helper (line 273); external=85, StartDolt=76 |
| `spec/cardb20/b02_delta_native_test.go` | covered via centralized StartDolt helper (line 273); external=351, StartDolt=318 |
| `spec/cardb20/b02_native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=145, StartDolt=116 |
| `spec/cardb20/b06_blocking_native_test.go` | covered via centralized StartDolt helper (line 273); external=-, StartDolt=302 |
| `spec/cardb20/b06_mixed_native_test.go` | covered via centralized StartDolt helper (line 273); external=-, StartDolt=124 |
| `spec/cardb20/b06_native_support_test.go` | covered via centralized StartDolt helper (line 273); external=105, StartDolt=- |
| `spec/cardb20/b06_native_test.go` | covered via centralized StartDolt helper (line 273); external=-, StartDolt=61 |
| `spec/cardb20/b13_memory_native_test.go` | covered via centralized StartDolt helper (line 273); external=219, StartDolt=210 |
| `spec/cardb20/native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=212, StartDolt=201 |
| `spec/cardb20/vocabulary_native_test.go` | covered via centralized StartDolt helper (line 273); external=289, StartDolt=278 |
| `spec/cardb21/native_test.go` | covered via centralized StartDolt helper (line 273); external=326, StartDolt=298 |
| `spec/cardb24/native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=239, StartDolt=224 |
| `spec/cardb24/native_timer_import_test.go` | covered via centralized StartDolt helper (line 273); external=49, StartDolt=35 |
| `spec/cardb28native/structured_hooks_test.go` | covered via centralized StartDolt helper (line 273); external=217, StartDolt=198 |
| `spec/s1-b01-b04/b04_ordinary_native_test.go` | covered via centralized StartDolt helper (line 273); external=396, StartDolt=379 |
| `spec/s1-b01-b04/native_b02_recording_test.go` | covered via centralized StartDolt helper (line 273); external=362, StartDolt=343 |
| `spec/s1-b01-b04/native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=273, StartDolt=251 |
| `spec/s1-b05-b08/native_recording_b07_test.go` | covered via centralized StartDolt helper (line 273); external=116, StartDolt=89 |
| `spec/s1-b09-native/domain_recording_test.go` | covered via centralized StartDolt helper (line 273); external=39, StartDolt=29 |
| `spec/s1-b13-native/recording_test.go` | covered via centralized StartDolt helper (line 273); external=120, StartDolt=103 |
| `spec/s1-b21-b24/native_count_matrix_test.go` | covered via centralized StartDolt helper (line 273); external=59, StartDolt=211 |
| `spec/s1-b21-b24/native_recording_test.go` | covered via centralized StartDolt helper (line 273); external=567, StartDolt=525 |
| `spec/s1-b25-b28/public_recording_test.go` | covered via centralized StartDolt helper (line 273); external=825, StartDolt=130 |
