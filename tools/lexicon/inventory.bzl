"""Declare the complete checkout as source inputs, across Bazel packages."""

def _verified_execution_root(ctx, root):
    # The directory of this repository is supplied by Bazel, not by a source
    # symlink. Bind the exception to that output base, and verify Bazel's own
    # workspace marker before trusting its output layout (Bazel 9 / Bzlmod).
    external = ctx.path(".").dirname
    if external.basename != "external":
        fail("lexicon inventory: cannot verify Bazel's external repository directory")
    output_base = external.dirname.realpath
    marker = output_base.get_child("DO_NOT_BUILD_HERE")
    if ctx.read(marker, watch = "yes") != str(root):
        fail("lexicon inventory: Bazel output base does not identify this workspace")
    execution_root = output_base.get_child("execroot", "_main")
    if not execution_root.is_dir or execution_root.realpath != execution_root:
        fail("lexicon inventory: cannot verify Bazel's execution root")
    return execution_root

def _is_output_link_candidate(entry, target, root, execution_root):
    if not target.is_dir:
        return False
    target_name = str(target)
    execution_name = str(execution_root)

    # Each conventional name has its own target. In particular bazel-bin
    # cannot stand in for the execution root, output root, or test logs.
    if entry.basename == "bazel-" + root.basename:
        return target_name == execution_name
    outputs = execution_root.get_child("bazel-out")
    if entry.basename == "bazel-out":
        return target_name == str(outputs)
    kinds = {"bazel-bin": "bin", "bazel-testlogs": "testlogs"}
    kind = kinds.get(entry.basename)

    # Check the output root and kind now; the mandatory configured rule below
    # also checks the exact configuration using Bazel's ctx.bin_dir metadata.
    if kind == None or target.basename != kind:
        return False
    return str(target.dirname.dirname) == str(outputs)

def _lexicon_output_links_impl(ctx):
    expected = {
        "bazel-out": ctx.attr.execution_root + "/bazel-out",
        "bazel-bin": ctx.attr.execution_root + "/" + ctx.bin_dir.path,
        "bazel-testlogs": ctx.attr.execution_root + "/" + ctx.bin_dir.path.removesuffix("/bin") + "/testlogs",
    }
    expected["bazel-" + ctx.attr.checkout_name] = ctx.attr.execution_root
    for name, target in ctx.attr.links.items():
        if not ctx.attr.execution_root or target != expected.get(name):
            fail("lexicon inventory: source symlink %s is unsupported; target %s does not match Bazel output metadata %s" % (name, target, expected.get(name)))
    return [DefaultInfo()]

lexicon_output_links = rule(
    implementation = _lexicon_output_links_impl,
    attrs = {
        "checkout_name": attr.string(mandatory = True),
        "execution_root": attr.string(mandatory = True),
        "links": attr.string_dict(mandatory = True),
    },
)

def _lexicon_sources_impl(ctx):
    root = ctx.path(ctx.attr.root_file).dirname
    pending = [(root, "")]
    sources = {}
    execution_root = None
    output_links = {}
    output_names = ["bazel-bin", "bazel-out", "bazel-testlogs", "bazel-" + root.basename]

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

            # Watch the path before excluding it. Directory listings alone
            # do not invalidate an inventory when only a link is retargeted.
            ctx.watch(entry)
            if entry.realpath != entry:
                if not prefix and entry.basename in output_names:
                    if execution_root == None:
                        execution_root = _verified_execution_root(ctx, root)
                    if _is_output_link_candidate(entry, entry.realpath, root, execution_root):
                        output_links[entry.basename] = str(entry.realpath)
                        continue
                fail("lexicon inventory: source symlink %s is unsupported; use regular source files/directories" % relative)
            if entry.is_dir:
                pending.append((entry, relative + "/"))
            else:
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

    # Loading this data in a separate source package avoids a load cycle with
    # this repository's root_file and rule definition. //:lexicon_tree requires
    # that package's configured check, so candidates are never final exclusions
    # based solely on a path shape (even inside the genuine output base).
    ctx.file("output_links.bzl", """CHECKOUT_NAME = %r
EXECUTION_ROOT = %r
OUTPUT_LINKS = %s
""" % (root.basename, str(execution_root) if execution_root else "", repr(output_links)), executable = False)
    ctx.file("BUILD.bazel", """exports_files(["source_inventory.json", "output_links.bzl"])
filegroup(
    name = "tree",
    srcs = ["source_inventory.json"] + glob(["files/*"]),
    visibility = ["//visibility:public"],
)
""", executable = False)

lexicon_sources = repository_rule(
    implementation = _lexicon_sources_impl,
    attrs = {"root_file": attr.label(mandatory = True, allow_single_file = True)},
    # Persisted file-watch markers record a directory as just DIR, losing a
    # symlink's target. Do not reuse that coarse marker after a watched path
    # changes or the server restarts; refetch this local checkout inventory.
    local = True,
)
