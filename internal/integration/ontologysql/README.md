# Ontology SQL integration

This target composes the stored ontology operations and canonical RPC bindings
with the real Writer, service, and SQL drivers. Each run creates a file-backed
SQLite store and a disposable PostgreSQL database, seeds the protected source
through the trusted fixture seam, and writes bead rows through the production
service and Writer.

Run it with:

```sh
bazel --ignore_all_rc_files test //internal/integration/ontologysql:ontologysql_test \
  --disk_cache=/tmp/plug-swarm/bazel-disk-cache \
  --local_resources=memory=8192 --jobs=2 --test_output=all --test_timeout=900
```

The assertions cover base and overlay reads, schema and integrity results,
source-only history and historical diff snapshots, detached candidate plans,
row/source invalidation, protected/missing/malformed source refusals, malformed
stored bead refusal, generic protected-record writes, and the deliberately
unbound `ontology.apply` method. Migration-size and validating-pre callback
qualification are owned by the migration integration lane.
