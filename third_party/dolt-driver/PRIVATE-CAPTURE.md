Private capture spike only. Upstream driver/v2 v2.2.0 source at
1e1c2f67f87291a3fae2a5b490a3c0c987cd6868, Apache-2.0. All original production
Go files are retained byte-for-byte. physical_commit.go is the only added API.
No upstream publication or Beads storage/semantic change is authorized.

ReadPhysicalCommit takes an exact immutable hash and a dedicated database/sql
connection. It owns a serializable read-only transaction, one registered query,
and a single resolved commit/root. It enumerates physical tables from that root,
constructs each table from its own schema, locks it to that root, and uses the
existing driver value conversion. No nonlocal or helper-name lookup is used.
Binary buffers are copied, duplicate keyless rows and NULLs are retained. All
iterators close before leaving Raw, then the transaction rolls back. Only
materialized driver values and driver-owned errors escape the operation.

The private operation requires the embedded connector's local root principal.
It refuses nondefault schemas instead of silently omitting them. Resolution,
permission, schema/row/decoding/read and cancellation failures are errors; only
a successful physical census permits a caller's present=false result. The
current public spike adapter remains test-only. Producer owner2f integrates
any reviewed winner; this branch does not change production capture paths.
