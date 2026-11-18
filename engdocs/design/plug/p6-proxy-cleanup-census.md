# P6 Beads proxy cleanup census

This bounded census accounts for all 32 consumer files in review B. Citations were regenerated from committed source `e20a5c147f47c1eaef7e155844450e3ddafcd25d` using `git grep -n` and `git show`. The centralized `StartDolt` helper registers cleanup for the real `s.DataRoot` at `internal/publicnative/server_linux.go:275`. `Server.Stop` drains owned proxies before reaping Dolt; existing direct cleanup registrations may perform an additional scan.

| Site | Evidence |
|---|---|
| `internal/integration/b03catalog/native_catalog_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 353; external init line(s) 608 |
| `internal/integration/b03publiccatalog/native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 154 |
| `internal/integration/b04claimidentity/native_b04_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 169; external init line(s) 192 |
| `spec/cardb01/canonicalfront/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 385 |
| `spec/cardb07/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 179 |
| `spec/cardb07effects/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 117 |
| `spec/cardb08/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 257 |
| `spec/cardb11/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 178 |
| `spec/cardb16/native_reference_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 87 |
| `spec/cardb17b18/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 76 |
| `spec/cardb20/b02_delta_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 318 |
| `spec/cardb20/b02_native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 116 |
| `spec/cardb20/b06_blocking_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 302; external init occurs in shared `b06_native_support_test.go:105` |
| `spec/cardb20/b06_mixed_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 124; external init occurs in shared `b06_native_support_test.go:105` |
| `spec/cardb20/b06_native_support_test.go` | covered via owned server passed into `workspace` at line 78, then `server.ChildEnv` at line 98; StartDolt callers: `b06_blocking_native_test.go:302`, `b06_mixed_native_test.go:124`, `b06_native_test.go:61` |
| `spec/cardb20/b06_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 61; external init occurs in shared `b06_native_support_test.go:105` |
| `spec/cardb20/b13_memory_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 210 |
| `spec/cardb20/native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 201 |
| `spec/cardb20/vocabulary_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 278 |
| `spec/cardb21/native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 298 |
| `spec/cardb24/native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 224 |
| `spec/cardb24/native_timer_import_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 35 |
| `spec/cardb28native/structured_hooks_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 198 |
| `spec/s1-b01-b04/b04_ordinary_native_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 379; external init line(s) 396 |
| `spec/s1-b01-b04/native_b02_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 343; external init line(s) 362 |
| `spec/s1-b01-b04/native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 251; external init line(s) 273 |
| `spec/s1-b05-b08/native_recording_b07_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 89; external init line(s) 116 |
| `spec/s1-b09-native/domain_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 29 |
| `spec/s1-b13-native/recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 103; external init line(s) 120 |
| `spec/s1-b21-b24/native_count_matrix_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 211; external init line(s) 59 |
| `spec/s1-b21-b24/native_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 525; external init line(s) 567 |
| `spec/s1-b25-b28/public_recording_test.go` | covered via `internal/publicnative/server_linux.go:275`; StartDolt call line(s) 130; external init line(s) 822 |
