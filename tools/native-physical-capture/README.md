# Disposable native physical capture

This private capture companion reads every retained immutable Dolt commit through
the storage driver's physical-image operation. It preserves each image's own
tables, columns, row multiplicity, NULLs and binary values without resolving
current nonlocal-table aliases. Current working-set rows, inventory, commit
metadata, sidecars and portable history still use the existing capture code.

Build `//tools/native-physical-capture:native-physical-capture` with the repository's
Bazel toolchain. Run the resulting binary only on a quiesced disposable copy:

```sh
cd /absolute/disposable/workspace
BEADS_DIR=/absolute/disposable/workspace/.beads \
  /absolute/path/native-physical-capture \
  --disposable \
  --data-dir=/absolute/disposable/dolt-data \
  --database=exact_database \
  --beads-dir=/absolute/disposable/workspace/.beads \
  --output-dir=/absolute/disposable/output
```

The data, Beads and output directories must already exist, have owner-only
permissions and contain no symlink components. The working directory must be the
copied workspace containing the selected `.beads` directory. Set HOME and XDG
configuration paths to an isolated disposable home before invocation; inventory
captures environment and configuration from those locations. The output directory
must contain none of `inventory.json`, `inventory.json.sources` or `capture.tar`.
The default deadline is 20 minutes; `--timeout` accepts a Go duration.

On success, the tool publishes the validated `capture.tar`, `inventory.json` and
`inventory.json.sources/` sidecars, then reports current-family row counts. Retain
the actual manifest and history ranges: complete native physical history does not
upgrade portable history that could not be mapped. Errors retain their typed
driver cause; they do not establish physical-table absence. A failed invocation
may leave sidecars; discard its disposable output rather than treating it as a
completed capture.

Capture includes source contents and is not a redaction mechanism. Keep private
inputs and outputs local, report only counts/categories, and clean up disposable
copies under the owning capture procedure. This executable changes no stock
`bd` command or Beads behavior.

The public route controls are selected explicitly because their targets are
manual:

```text
//internal/plugdump:physical_root_control_test
//cmd/plug-capture-sql-server:native_physical_capture_test
@com_github_dolthub_driver_v2//:physical_commit_test
```

Run them through the authorized remote Bazel wrapper with the native lane's
environment. Run `//internal/plugdump:plugdump_test` and the canonical whole gate
on the same revision. Public controls prove their fixture route only; source,
focused, whole, real-store delivery and official qualification remain separate.
