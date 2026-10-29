"""Materialize checksum-pinned dependencies for the real contract executable.

The maintenance generator verifies Go h1 checksums from the unchanged go.sum.
This repository rule verifies raw hashes while fetching the complete pinned
file proxy; acceptance needs no network or preexisting Go cache afterward.
"""

_BUILD = """\
exports_files(["proxy/ROOT"], visibility = ["//visibility:public"])

filegroup(
    name = "proxy",
    srcs = glob(["proxy/**"]),
    visibility = ["//visibility:public"],
)
"""

def _escape(value):
    out = ""
    for char in value.elems():
        out += "!" + char.lower() if char.isupper() else char
    return out

def _go_module_proxy_impl(rctx):
    contents = rctx.read(rctx.attr.lock)
    lock = json.decode(contents)
    if lock["schema"] != "plug-authz-contract-modules/v1":
        fail("unsupported authz contract module lock schema")
    seen = {}
    for module in lock["modules"]:
        key = module["path"] + "@" + module["version"]
        if key in seen or "mod_sha256" not in module:
            fail("duplicate or incomplete module lock record: " + key)
        seen[key] = True
        rel = "{}/@v/{}".format(_escape(module["path"]), _escape(module["version"]))
        for extension, field in (("mod", "mod_sha256"), ("zip", "zip_sha256")):
            if field not in module:
                continue
            url = "{}/{}.{}".format(rctx.attr.proxy_url.rstrip("/"), rel, extension)
            rctx.download(
                url = url,
                output = "proxy/{}.{}".format(rel, extension),
                sha256 = module[field],
                canonical_id = url,
            )
        rctx.file("proxy/{}.info".format(rel), json.encode({"Version": module["version"]}) + "\n")

    # The explicit marker also binds the materialized proxy to the source lock.
    rctx.file("proxy/ROOT", contents)
    rctx.file("BUILD.bazel", _BUILD)

go_module_proxy = repository_rule(
    implementation = _go_module_proxy_impl,
    attrs = {
        "lock": attr.label(mandatory = True, allow_single_file = True),
        "proxy_url": attr.string(
            default = "https://proxy.golang.org",
            doc = "Pinned artifact origin; file:// is useful for isolated repository-rule diagnostics. Hash checks always apply.",
        ),
    },
    doc = "Fetch the raw-hash-verified offline proxy for the unchanged authz module.",
)
