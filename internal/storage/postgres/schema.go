package postgres

// All SQL objects are private to this driver in a dedicated store database.
// Decimal counters cover the complete uint64 domain, including epochs above
// MaxInt64. Every Go conversion and increment is checked before publication.
const schemaDDL = `
CREATE SCHEMA IF NOT EXISTS plug_postgres;
CREATE TABLE IF NOT EXISTS plug_postgres.meta (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 store_id text NOT NULL, incarnation text NOT NULL,
 schema_version bigint NOT NULL CHECK (schema_version = 1),
 head numeric(20,0) NOT NULL CHECK (head BETWEEN 0 AND 18446744073709551615),
 history_floor numeric(20,0) NOT NULL CHECK (history_floor BETWEEN 0 AND head)
);
CREATE TABLE IF NOT EXISTS plug_postgres.lease (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 epoch numeric(20,0) NOT NULL CHECK (epoch BETWEEN 0 AND 18446744073709551615),
 owner text NOT NULL, holder text NOT NULL, expires timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS plug_postgres.locks (key bytea PRIMARY KEY);
CREATE TABLE IF NOT EXISTS plug_postgres.records (
 key bytea PRIMARY KEY, ref bytea NOT NULL, kind text NOT NULL,
 revision numeric(20,0) NOT NULL, body bytea, deleted boolean NOT NULL,
 version_count numeric(20,0) NOT NULL
);
CREATE TABLE IF NOT EXISTS plug_postgres.versions (
 key bytea NOT NULL, revision numeric(20,0) NOT NULL,
 is_base boolean NOT NULL, value bytea NOT NULL,
 PRIMARY KEY (key, revision)
);
CREATE TABLE IF NOT EXISTS plug_postgres.states (
 key bytea NOT NULL, revision numeric(20,0) NOT NULL, value bytea NOT NULL,
 PRIMARY KEY (key, revision)
);
CREATE TABLE IF NOT EXISTS plug_postgres.blobs (
 hash text PRIMARY KEY, revision numeric(20,0) NOT NULL, value bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS plug_postgres.changesets (
 revision numeric(20,0) PRIMARY KEY, value bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS plug_postgres.receipts (
 scope bytea PRIMARY KEY, revision numeric(20,0) NOT NULL,
 expires timestamptz NOT NULL, value bytea NOT NULL,
 finalized boolean NOT NULL DEFAULT false, post bytea
);
CREATE TABLE IF NOT EXISTS plug_postgres.checkpoints (
 key bytea PRIMARY KEY,
 generation numeric(20,0) NOT NULL CHECK (generation BETWEEN 1 AND 18446744073709551615),
 payload bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS plug_postgres.core_sessions (
 id text PRIMARY KEY CHECK (char_length(id)=32), token_digest bytea NOT NULL,
 principal_kind text NOT NULL CHECK (principal_kind='user'), principal_id text NOT NULL,
 expires_at timestamptz NOT NULL, revoked boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS core_sessions_active_expiry
 ON plug_postgres.core_sessions(revoked, expires_at);
CREATE TABLE IF NOT EXISTS plug_postgres.allocator_states (
 prefix text PRIMARY KEY, data bytea NOT NULL
);`

// allocator_states is intentionally additive to schema version 1. initialize
// executes schemaDDL for every open, so an existing version-1 database gains
// the durable allocator table without rewriting any existing data.
