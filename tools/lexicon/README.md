# Lexicon source inventory

Run the required tree gate with:

```sh
bazel test //:lexicon_gate --lockfile_mode=error --test_output=all
```

`inventory.bzl` walks the checkout, including hidden directories and every
Bazel package, and declares each file as an individual Bazel source input.
Directory listings and files are watched so additions, removals, and new
package boundaries invalidate the inventory after a cached pass. The generated
repository uses flat artifact names; a source `BUILD` or `BUILD.bazel` never
forms an unexamined package boundary. The test reads only those declared
runfiles and a generated path mapping. It does not read the checkout, invoke
Git, or use the network inside its sandbox.

`source_manifest.txt` is the reviewed list of all source paths. The gate fails
with sorted `missing` and `extra` paths when it differs from the checkout.
All ordinary files are source, even ignored or untracked files. Only the root
`.git` metadata entry and actual Bazel convenience symlinks are omitted.
Other symlinks fail closed with their source path; use regular source files
and directories. Ordinary paths named `bazel-*` are still source. Generated
outputs should be kept outside the checkout or in Bazel's output directories.

For an intentional addition, removal, or new package:

1. Add/remove the source and review the diagnostic paths. Add/remove exactly
   those repository-relative paths in `source_manifest.txt`, keeping it sorted
   with one path per line. Include package metadata, docs, and hidden files.
2. Run the required gate again. Listing a file only admits it to vocabulary
   checking; it never exempts its contents. New packages require no manual
   connection to `//:lexicon_tree`.
3. Keep the existing ownership rules. `.github/` and
   `engdocs/design/plug/` remain explicit protected-owner exceptions in
   `policy.go`; their text is skipped, and declared binaries must match their
   hashes. Their owner must separately accept policy entries for additions.
   ADRs retain the lexicon's provenance rules. Inventory admission grants no
   permission to edit any of these owners' files.

The standalone `lexicon_check` command checks the Git-tracked inventory and
the same manifest/policy. The required Bazel target also sees untracked source
paths and checks package-independent completeness.
