package sqlite

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
 finalized INTEGER NOT NULL DEFAULT 0, post BLOB
);
CREATE TABLE IF NOT EXISTS checkpoints (key TEXT PRIMARY KEY, generation TEXT NOT NULL, payload BLOB);
`
