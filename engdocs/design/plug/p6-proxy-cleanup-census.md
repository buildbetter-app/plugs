# P6 Beads proxy cleanup census

Every native test site that constructs `init --server --external` is listed below.

| Site | Status |
|---|---|
| `spec/s1-b01-b04/native_recording_test.go` | registered |
| `spec/s1-b01-b04/native_b02_recording_test.go` | registered |
| `internal/integration/b03publiccatalog/native_recording_test.go` | registered |
| `spec/cardb28native/structured_hooks_test.go` | registered |
| `spec/s1-b25-b28/public_recording_test.go` source and target | registered |
| `spec/cardb01/canonicalfront/native_test.go` | registered |
| `spec/cardb20/b02_native_recording_test.go` | registered |
| `spec/cardb20/b02_delta_native_test.go` | registered |

No-proxy sites: all other native recorder tests use an owned server without
external Beads initialization, so no proxy is started.
