load("@rules_go//go:def.bzl", "go_test")
load(":test_common.bzl", "DEPS")

# B06 owns these additive targets; the existing B20 recipe stays unchanged.
B06_SRCS = ["b06_cases_test.go", "b06_plug_test.go", "postgres_test.go"]
B06_DEPS = DEPS + [
    "//internal/core/restore",
    "//internal/server/transport/httpjson",
    "//internal/storage/conformance",
]

def b06_tests():
    """Declare the existing B06 targets without changing their configuration."""
    go_test(
        name = "b06_cycles_test",
        srcs = B06_SRCS,
        args = ["-test.v"],
        deps = B06_DEPS,
        size = "large",
        tags = ["local", "requires-network"],
        timeout = "long",
    )

    go_test(
        name = "b06_native_cycles_test",
        srcs = B06_SRCS + [":native_build_support", "b06_native_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB06CanonicalNativeCycles$$"],
        deps = B06_DEPS + ["//internal/publicnative"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )
