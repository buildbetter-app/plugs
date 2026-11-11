# Disposable native backup restore

This private entrypoint calls the existing normal-driver
`versioncontrolops.BackupRestore` API. It accepts an already copied Dolt-native
backup, restores it into a new database under an empty disposable data directory,
and authenticates the restored immutable HEAD with the existing physical reader.
It does not change Beads commands, capture code, dependencies or engine semantics.

Build `//tools/native-backup-restore:native-backup-restore` with the authorized
remote Bazel wrapper. Run the exact delivered executable with isolated HOME,
DOLT_ROOT_PATH and XDG configuration, against owned disposable paths:

```sh
/absolute/path/native-backup-restore --disposable \
  --backup-dir=/absolute/disposable/backup \
  --data-dir=/absolute/disposable/empty-data \
  --database=capture
```

Both directories must exist, have owner-only permissions, be disjoint and resolve
without symlinks. The backup must contain a native nonempty manifest. The data
directory must be empty. There is no force or overwrite mode. A failed operation
can leave scratch data for the caller to inspect and remove; never reuse it as a
completed restore. The default deadline is twenty minutes.

Success verifies a real immutable HEAD, not complete current working-set retention
or migration fidelity. Run the separately bound, unchanged capture executable and
compare the backup's retained unversioned families under the owning procedure.
Keep private sources/results local and report only counts/categories. This tool
never copies or locks a live source. The backup-copy lifetime and original
manifest binding belong to the sole private capture owner.

`//tools/native-backup-restore:native_backup_restore_test` creates a public native
backup, invokes the real executable, compares the complete commit graph and every
retained physical image/schema/row/NULL/binary/duplicate, and reports independently
whether the ignored current audit rows survive. It also requires an occupied
scratch directory to refuse without modifying its sentinel. Its artifacts retain
the exact executable and source-bound public receipt; private use requires that
actual delivery, never source-only confidence.
