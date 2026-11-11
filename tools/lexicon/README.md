# Lexicon source inventory

Run the required tree gate with:

```sh
bazel test //:lexicon_gate --lockfile_mode=error --test_output=all
```

`inventory.bzl` walks the checkout, including hidden directories and every
Bazel package, and declares each file as an individual Bazel source input.
Directory listings and each entry (including excluded output symlinks) are
watched so additions, removals, link retargeting, and new package boundaries
invalidate the inventory after a cached pass. This is a local repository rule:
Bazel's persisted file-watch marker represents a directory as just `DIR`, so
reusing that marker could hide a directory-to-directory link retarget. Local
reevaluation preserves the watched path's target changes and also refreshes
the inventory after server restarts. The generated
repository uses flat artifact names; a source `BUILD` or `BUILD.bazel` never
forms an unexamined package boundary. The test reads only those declared
runfiles and a generated path mapping. It does not read the checkout, invoke
Git, or use the network inside its sandbox.

`source_manifest.txt` and the area lists in `sources.d/*.txt` together form the
reviewed list of all source paths. The base manifest explicitly lists each
area file; an area file cannot register itself or another area file. Area
files use the same one-path-per-line format and load in lexical filename
order. Duplicate paths fail across the entire inventory, including the base
and area lists. Invalid paths, invalid UTF-8 and empty lists also fail.
The gate reports sorted `missing` and `extra` paths when the combined list
differs from the checkout. Membership never exempts a file from content checks.
All ordinary files are source, even ignored or untracked files. Only the root
`.git` metadata entry and actual Bazel convenience symlinks are omitted.

The pinned public companion's config-list command needs one external
environment assignment. `tools/lexicon/rules.d/b25-b28-public.toml` records its exact decoded
Go string and the single recorder source path. The checker admits that string
literal only at that path; identifiers, imports, other literals, and all other
lexicon findings in the same file remain subject to the ordinary gate. This
interop allowance does not admit a prefix, a family of environment names, or
the rest of the source file.
The output-link exception is bound to the current Bazel output base: the
repository rule locates its own Bazel-supplied external-repository directory
and verifies the output base's workspace marker. `bazel-out` must resolve to
that base's `execroot/_main/bazel-out`, and `bazel-<checkout-name>` to its
`execroot/_main`. The required `output_links:verified` dependency also compares
each recorded link against Bazel's configured `ctx.bin_dir.path` metadata.
`bazel-bin` and `bazel-testlogs` must resolve to the exact current configuration's
`bin` and `testlogs` directories, respectively, under that verified root.
A matching basename or an output-shaped path elsewhere is insufficient.
This supports Bazel 9's default convenience links; clear stale links before
switching output bases, configurations, or using a different symlink prefix.
Other symlinks fail closed with their source path; use regular source files
and directories. Ordinary paths named `bazel-*` are still source. Generated
outputs should be kept outside the checkout or in Bazel's output directories.

For an intentional addition, removal, or new package:

1. Add/remove the source and review the diagnostic paths. Add/remove exactly
   those repository-relative paths in the area's `sources.d/*.txt` file, or
   `source_manifest.txt` for other areas, keeping each list sorted with one
   path per line. `wire.txt` owns `internal/api/wire/` and `cardb20.txt` owns
   `spec/cardb20/`. Include package metadata, docs, and hidden files. Register
   any new area list in the base manifest and keep each source in exactly one
   list. Update a source's existing list rather than copying another branch's
   whole manifest; independent additions to other areas must survive.
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

Immutable recorded reference inputs have a narrower admission boundary than
the ordinary provenance prose zones. `TreePolicy` lists their exact path and
SHA-256 under `ReferencePrefixes`; the checker skips vocabulary scanning only
when both the path and bytes match. A changed reference or an additional path
under that prefix fails closed. The current recorded package admits only the
exact `spec/recorded/goldens.json` path; its validator source remains an
ordinary scanned file, so new product vocabulary there is still rejected.

Area rules live in `rules.d/*.toml` and load in lexical filename order. Each
file starts its own TOML section; terms and provenance scopes combine across
files, while repeated singleton allowances and duplicate native sites fail
closed. `00-vocabulary.toml` holds the global term list. Provenance scopes live in
`provenance.toml`, `migration.toml` and `offline-history.toml`; each native
recorder area owns its exact protocol data.
The same reviewed files are embedded in the checker to bind accepted paths,
complete literal sets and context tuples. Changing a runtime copy cannot
broaden those bindings. Only the exact rule bytes consumed by the loader are
exempt from vocabulary scanning; the complete source inventory and all other
files still pass through the existing tree policy. The standalone command and
hermetic gate use the same loader, including the gate's flat runfile mapping.
