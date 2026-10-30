"""Expose the resolved rules_go SDK to subprocess behavior tests."""

def _test_go_sdk_impl(ctx):
    sdk = ctx.toolchains["@rules_go//go:toolchain"].sdk
    return [DefaultInfo(
        files = depset([sdk.go]),
        runfiles = ctx.runfiles(transitive_files = depset(
            [sdk.go],
            transitive = [sdk.headers, sdk.srcs, sdk.libs, sdk.tools],
        )),
    )]

test_go_sdk = rule(
    implementation = _test_go_sdk_impl,
    toolchains = ["@rules_go//go:toolchain"],
)
