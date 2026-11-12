load("@rules_go//go:def.bzl", "go_test")
load(":test_common.bzl", "COMMON", "DEPS")

def b20_tests():
    """Declare the existing B20 targets without changing their configuration."""
    go_test(
        name = "current_test",
        srcs = COMMON,
        args = ["-test.v"],
        deps = DEPS,
        size = "large",
        tags = ["local", "requires-network"],
        timeout = "long",
    )

    # Explicit public source-built reference recordings; no official producer action.
    go_test(
        name = "record_native_test",
        srcs = COMMON + ["native_build_test.go", "native_recording_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB20FreshNativeCurrentWorkingSetTwins$$"],
        deps = DEPS + ["//internal/publicnative"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )

    native.filegroup(
        name = "lexicon_files",
        srcs = native.glob(["**"]),
        visibility = ["//visibility:public"],
    )

    # Reuse the unchanged canonical source-build recipe in card-specific targets.
    native.filegroup(
        name = "native_build_support",
        srcs = ["native_build_test.go"],
        visibility = ["//visibility:public"],
    )

    # Reuse the existing public disposable PostgreSQL fixture without copying it.
    native.filegroup(
        name = "native_postgres_support",
        srcs = ["postgres_test.go"],
        visibility = ["//visibility:public"],
    )

    # New exact caller/reconnect/reopen assertions paired with two fresh references.
    go_test(
        name = "record_caller_closure_test",
        srcs = COMMON + ["native_build_test.go", "native_recording_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB20NativeCallerCommitOrderAndReopen$$"],
        deps = DEPS + ["//internal/publicnative"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )

    # Complete closed native vocabulary, with independent three-engine callers.
    go_test(
        name = "record_vocabulary_test",
        srcs = COMMON + ["native_build_test.go", "native_recording_test.go", "vocabulary_native_test.go"],
        data = ["//spec/s1-b09-native:lexicon_files", "//internal/publicnative:dolt_runtime"],
        env = {"PLUG_PINNED_DOLT_BINARY": "$(rlocationpath //internal/publicnative:dolt_runtime)"},
        args = ["-test.v", "-test.run=^TestB20NativeMutationVocabularyTwins$$"],
        deps = DEPS + ["//internal/publicnative"],
        size = "enormous",
        tags = ["local", "manual"],
        timeout = "eternal",
    )
