load("@rules_go//go:def.bzl", "go_test")
load(":test_common.bzl", "COMMON", "DEPS")

def b13_tests():
    """Declare the existing B13 targets without changing their configuration."""
    # B13 public operational vectors reuse the exact B20 source-build and recorder
    # support. Existing B20 targets and helper bytes remain unchanged.
    go_test(
        name = "b13_memory_record_native_test",
        srcs = COMMON + ["native_build_test.go", "native_recording_test.go", "b13_memory_native_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB13CanonicalPublicMemoryCounterparts$$"],
        deps = DEPS + ["//internal/publicnative", "//internal/storage/conformance"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )
