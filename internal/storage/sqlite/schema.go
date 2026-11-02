package sqlite

const schemaVersion = 2

// All revisions sort as fixed-width unsigned decimal TEXT. SQLite INTEGER is
// signed; binding a uint64 or casting a revision to INTEGER would lose data.
const schema = `
CREATE TABLE IF NOT EXISTS meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_id TEXT NOT NULL,
 incarnation TEXT NOT NULL, schema_version INTEGER NOT NULL,
 head TEXT NOT NULL, history_floor TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS active_lease (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), epoch TEXT NOT NULL,
 token TEXT NOT NULL, holder TEXT NOT NULL, expires INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, kind TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS objects_kind_key ON objects(kind, key);
CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS versions (
 key TEXT NOT NULL, rev TEXT NOT NULL, base INTEGER NOT NULL, data BLOB NOT NULL,
 PRIMARY KEY(key, rev)
);
CREATE TABLE IF NOT EXISTS key_history (
 key TEXT NOT NULL, rev TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(key, rev)
);
CREATE TABLE IF NOT EXISTS blobs (hash TEXT PRIMARY KEY, rev TEXT NOT NULL, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS changesets (rev TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS replays (
 scope TEXT PRIMARY KEY, created INTEGER NOT NULL, rev TEXT NOT NULL,
 request_hash TEXT NOT NULL, response BLOB NOT NULL, changeset BLOB,
 finalized INTEGER NOT NULL DEFAULT 0, post BLOB,
 pending INTEGER NOT NULL DEFAULT 0 CHECK(pending IN (0,1))
);
CREATE TABLE IF NOT EXISTS checkpoints (key TEXT PRIMARY KEY, generation TEXT NOT NULL, payload BLOB);
CREATE TABLE IF NOT EXISTS core_sessions (
 id TEXT PRIMARY KEY CHECK(length(id)=32), token_digest BLOB NOT NULL,
 principal_kind TEXT NOT NULL CHECK(principal_kind='user'), principal_id TEXT NOT NULL,
 expires_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1))
);
CREATE INDEX IF NOT EXISTS core_sessions_active_expiry ON core_sessions(revoked, expires_at);
CREATE TABLE IF NOT EXISTS allocator_states (prefix TEXT PRIMARY KEY, data BLOB NOT NULL);
`
