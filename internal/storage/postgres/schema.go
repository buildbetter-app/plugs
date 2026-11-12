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
-- Publication protects ownership, while the reserved heartbeat connection
-- may update expiry. PostgreSQL treats these unique-index columns as the row
-- key: KEY SHARE blocks Release/takeover, but permits expiry-only updates.
CREATE UNIQUE INDEX IF NOT EXISTS lease_ownership
 ON plug_postgres.lease(singleton,epoch,owner);
CREATE TABLE IF NOT EXISTS plug_postgres.locks (key bytea PRIMARY KEY);
CREATE TABLE IF NOT EXISTS plug_postgres.records (
 key bytea PRIMARY KEY, ref bytea NOT NULL, kind text NOT NULL,
 revision numeric(20,0) NOT NULL, body bytea, deleted boolean NOT NULL,
 version_count numeric(20,0) NOT NULL
);
-- Current-head Query pages only live records. These partial indexes preserve
-- key order for both the unfiltered and kind-filtered keyset predicates while
-- remaining additive for databases created by the main schema.
CREATE INDEX IF NOT EXISTS records_live_key
 ON plug_postgres.records(key) WHERE NOT deleted;
CREATE INDEX IF NOT EXISTS records_live_kind_key
 ON plug_postgres.records(kind,key) WHERE NOT deleted;
CREATE TABLE IF NOT EXISTS plug_postgres.versions (
 key bytea NOT NULL, revision numeric(20,0) NOT NULL,
 is_base boolean NOT NULL, value bytea NOT NULL, query_fields jsonb,
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
);

-- Representation does not imply coverage. initialize never inserts a marker:
-- pre-index stores remain unbuilt until a complete typed publication.
CREATE TABLE IF NOT EXISTS plug_postgres.hierarchy_index_coverage (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 format integer NOT NULL CHECK (format = 1),
 built_from numeric(20,0) NOT NULL CHECK (built_from BETWEEN 0 AND 18446744073709551615),
 certified_through numeric(20,0) NOT NULL CHECK (certified_through BETWEEN built_from AND 18446744073709551615),
 state text NOT NULL CHECK (state IN ('unbuilt','ready')),
 PRIMARY KEY (incarnation, branch)
);
CREATE TABLE IF NOT EXISTS plug_postgres.hierarchy_source_current (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 child_key bytea NOT NULL, parent_key bytea,
 ref bytea NOT NULL, kind text NOT NULL CHECK (kind = 'bead'),
 revision numeric(20,0) NOT NULL CHECK (revision BETWEEN 0 AND 18446744073709551615),
 body bytea,
 PRIMARY KEY (incarnation, branch, child_key),
 CHECK (parent_key IS NULL OR parent_key <> child_key)
);
CREATE INDEX IF NOT EXISTS hierarchy_source_current_parent
 ON plug_postgres.hierarchy_source_current(incarnation, branch, parent_key, child_key)
 WHERE parent_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS plug_postgres.hierarchy_source_history (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 child_key bytea NOT NULL, parent_key bytea,
 start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615),
 end_revision numeric(20,0) CHECK (end_revision > start_revision AND end_revision <= 18446744073709551615),
 ref bytea NOT NULL, kind text NOT NULL CHECK (kind = 'bead'),
 revision numeric(20,0) NOT NULL CHECK (revision BETWEEN 0 AND start_revision),
 body bytea,
 PRIMARY KEY (incarnation, branch, child_key, start_revision),
 CHECK (parent_key IS NULL OR parent_key <> child_key)
);
CREATE INDEX IF NOT EXISTS hierarchy_source_history_parent
 ON plug_postgres.hierarchy_source_history(incarnation, branch, parent_key, child_key, start_revision)
 WHERE parent_key IS NOT NULL;

-- This format certifies complete typed outgoing Links, independently of Parent.
-- Source rows distinguish an explicit empty list from unknown metadata. Kind
-- bytes include custom values; only the separate guard selector limits kinds.
CREATE TABLE IF NOT EXISTS plug_postgres.graph_link_index_coverage (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 format integer NOT NULL CHECK (format = 1),
 built_from numeric(20,0) NOT NULL CHECK (built_from BETWEEN 0 AND 18446744073709551615),
 certified_through numeric(20,0) NOT NULL CHECK (certified_through BETWEEN built_from AND 18446744073709551615),
 state text NOT NULL CHECK (state IN ('unbuilt','ready')),
 PRIMARY KEY (incarnation, branch)
);
CREATE TABLE IF NOT EXISTS plug_postgres.graph_link_source_current (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 source_key bytea NOT NULL, ref bytea NOT NULL,
 kind text NOT NULL CHECK (kind = 'bead'),
 revision numeric(20,0) NOT NULL CHECK (revision BETWEEN 0 AND 18446744073709551615),
 body bytea,
 PRIMARY KEY (incarnation, branch, source_key)
);
CREATE TABLE IF NOT EXISTS plug_postgres.graph_link_current (
 incarnation text NOT NULL, branch numeric(20,0) NOT NULL,
 source_key bytea NOT NULL, link_kind bytea NOT NULL CHECK (octet_length(link_kind) BETWEEN 1 AND 32),
 target_key bytea NOT NULL,
 PRIMARY KEY (incarnation, branch, source_key, link_kind, target_key),
 FOREIGN KEY (incarnation, branch, source_key)
  REFERENCES plug_postgres.graph_link_source_current(incarnation, branch, source_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS graph_link_current_incoming
 ON plug_postgres.graph_link_current(incarnation, branch, target_key, link_kind, source_key);
CREATE TABLE IF NOT EXISTS plug_postgres.graph_link_source_history (
 incarnation text NOT NULL,
 branch numeric(20,0) NOT NULL CHECK (branch BETWEEN 0 AND 18446744073709551615),
 source_key bytea NOT NULL,
 start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615),
 end_revision numeric(20,0) CHECK (end_revision > start_revision AND end_revision <= 18446744073709551615),
 ref bytea NOT NULL, kind text NOT NULL CHECK (kind = 'bead'),
 revision numeric(20,0) NOT NULL CHECK (revision BETWEEN 0 AND start_revision),
 body bytea,
 PRIMARY KEY (incarnation, branch, source_key, start_revision)
);
CREATE TABLE IF NOT EXISTS plug_postgres.graph_link_history (
 incarnation text NOT NULL, branch numeric(20,0) NOT NULL,
 source_key bytea NOT NULL, start_revision numeric(20,0) NOT NULL,
 link_kind bytea NOT NULL CHECK (octet_length(link_kind) BETWEEN 1 AND 32), target_key bytea NOT NULL,
 PRIMARY KEY (incarnation, branch, source_key, start_revision, link_kind, target_key),
 FOREIGN KEY (incarnation, branch, source_key, start_revision)
  REFERENCES plug_postgres.graph_link_source_history(incarnation, branch, source_key, start_revision) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS graph_link_history_incoming
 ON plug_postgres.graph_link_history(incarnation, branch, target_key, link_kind, source_key, start_revision);
`

// allocator_states is intentionally additive to schema version 1. initialize
// executes schemaDDL for every open, so an existing version-1 database gains
// the durable allocator table without rewriting any existing data.
