"""Declare the complete checkout as source inputs, across Bazel packages."""

def _lexicon_sources_impl(ctx):
    root = ctx.path(ctx.attr.root_file).dirname
    pending = [(root, "")]
    sources = {}
    output_links = ["bazel-bin", "bazel-out", "bazel-testlogs", "bazel-" + root.basename]

    # Starlark does not permit recursion or while loops. This bound fails
    # closed rather than truncating an unexpectedly large source tree.
    for _ in range(100000):
        if not pending:
            break
        directory, prefix = pending.pop()

        # Watching every listing makes new directories and package markers
        # invalidate this repository even after a cached successful test.
        for entry in directory.readdir(watch = "yes"):
            relative = prefix + entry.basename
            if relative == ".git":
                continue
            if entry.realpath != entry:
                target = str(entry.realpath)

                # Only actual Bazel convenience symlinks are metadata. An
                # ordinary file/directory with one of these names is source.
                if not prefix and entry.basename in output_links and (target.endswith("/execroot/_main") or "/execroot/_main/bazel-out/" in target or target.endswith("/execroot/_main/bazel-out")):
                    continue
                fail("lexicon inventory: source symlink %s is unsupported; use regular source files/directories" % relative)
            if entry.is_dir:
                pending.append((entry, relative + "/"))
            else:
                ctx.watch(entry)
                sources[relative] = entry
    if pending:
        fail("lexicon inventory: directory limit exceeded; review tools/lexicon/inventory.bzl")

    records = []
    for index, relative in enumerate(sorted(sources)):
        # Flat names keep BUILD/BUILD.bazel in the source tree from creating
        # package boundaries in this generated repository.
        filename = "files/" + str(index)
        ctx.symlink(sources[relative], filename)
        records.append({"path": relative, "file": filename})
    ctx.file("source_inventory.json", json.encode({"version": 1, "files": records}), executable = False)
    ctx.file("BUILD.bazel", """exports_files(["source_inventory.json"])
filegroup(
    name = "tree",
    srcs = ["source_inventory.json"] + glob(["files/*"]),
    visibility = ["//visibility:public"],
)
""", executable = False)

lexicon_sources = repository_rule(
    implementation = _lexicon_sources_impl,
    attrs = {"root_file": attr.label(mandatory = True, allow_single_file = True)},
)
