load("@rules_go//go:def.bzl", "go_test")
load(":test_common.bzl", "COMMON", "DEPS")

# B02 owns these controls; existing B20 tests are compiled for unchanged store
# and native-command support but are not selected or rerun by either target.
B02_PUBLIC = ["b02_native_graph_test.go", "b02_public_graph_test.go"]
B02_DEPS = DEPS + [
    "//internal/core/ontology",
    "//internal/server/pluginhost",
    "//internal/storage/conformance",
    "//plugins/workflow",
]

def b02_tests():
    """Declare the existing B02 targets without changing their configuration."""
    go_test(
        name = "b02_public_blocking_test",
        srcs = COMMON + B02_PUBLIC,
        args = ["-test.v", "-test.run=^TestB02PublicBlockingAcrossStores$$"],
        deps = B02_DEPS,
        size = "large",
        tags = ["local", "requires-network"],
        timeout = "long",
    )

    go_test(
        name = "b02_native_blocking_test",
        srcs = COMMON + B02_PUBLIC + [":native_build_support", "native_recording_test.go", "b02_native_recording_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB02CanonicalNativeBlocking$$"],
        deps = B02_DEPS + ["//internal/publicnative"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )
