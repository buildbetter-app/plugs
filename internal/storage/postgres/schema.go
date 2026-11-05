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
-- Existing installations have no virgin-lifetime proof. Only the new-meta
-- INSERT explicitly sets this flag; emptiness never recreates it.
ALTER TABLE plug_postgres.meta ADD COLUMN IF NOT EXISTS selection_virgin boolean NOT NULL DEFAULT false;
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

-- Physical projections of existing typed sort_key/fact rows. These tables do
-- not certify an upgraded or restored store. Coverage is published with the
-- triggering transaction and is monotonically invalidated by untyped writes.
-- Scalar strings are bytea: NUL/non-UTF8 values retain Go bytewise semantics.
-- Byte-string overload of literal strpos; no wildcard/pattern interpretation.
CREATE OR REPLACE FUNCTION plug_postgres.strpos(bytea,bytea) RETURNS integer
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS 'SELECT position($2 in $1)';
CREATE TABLE IF NOT EXISTS plug_postgres.selection_coverage (
 incarnation text NOT NULL, start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615),
 end_revision numeric(20,0) CHECK (end_revision > start_revision AND end_revision <= 18446744073709551615), built_from numeric(20,0) NOT NULL,
 ready boolean NOT NULL, format integer NOT NULL DEFAULT 0,
 PRIMARY KEY (incarnation,start_revision)
);
CREATE TABLE IF NOT EXISTS plug_postgres.selection_sources (
 incarnation text NOT NULL, key bytea NOT NULL,
 start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615), end_revision numeric(20,0) CHECK (end_revision > start_revision AND end_revision <= 18446744073709551615),
 revision numeric(20,0) NOT NULL CHECK (revision BETWEEN 0 AND start_revision), present boolean NOT NULL,
 digest bytea NOT NULL, kind text NOT NULL,
 PRIMARY KEY (incarnation,key,start_revision)
);
CREATE TABLE IF NOT EXISTS plug_postgres.selection_rows (
 incarnation text NOT NULL, row_key bytea NOT NULL,
 start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615), end_revision numeric(20,0) CHECK (end_revision > start_revision AND end_revision <= 18446744073709551615),
 source_key bytea NOT NULL, source_revision numeric(20,0) NOT NULL CHECK (source_revision BETWEEN 0 AND start_revision),
 source_present boolean NOT NULL, source_digest bytea NOT NULL,
 bead_key bytea, ref bytea, bead_id bytea, bead_type bytea,
 project bytea, parent_key bytea, status bytea, title bytea,
 template_value boolean, priority bigint, leaf bytea, fact_kind text, annotation bytea NOT NULL,
 PRIMARY KEY (incarnation,row_key,start_revision)
);
-- Old draft layouts never receive version-2 certification by upgrade.
ALTER TABLE plug_postgres.selection_coverage ADD COLUMN IF NOT EXISTS format integer NOT NULL DEFAULT 0;
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS template_value boolean;
-- Legacy global markers retain their bytes but cannot satisfy any family.
ALTER TABLE plug_postgres.selection_coverage ADD COLUMN IF NOT EXISTS family_table text NOT NULL DEFAULT '';
ALTER TABLE plug_postgres.selection_coverage ADD COLUMN IF NOT EXISTS fact_kind text NOT NULL DEFAULT '';
ALTER TABLE plug_postgres.selection_coverage ADD COLUMN IF NOT EXISTS branch numeric(20,0) NOT NULL DEFAULT 0;
ALTER TABLE plug_postgres.selection_coverage ADD COLUMN IF NOT EXISTS certified_through numeric(20,0);
ALTER TABLE plug_postgres.selection_coverage DROP CONSTRAINT IF EXISTS selection_coverage_pkey;
ALTER TABLE plug_postgres.selection_coverage ADD PRIMARY KEY (incarnation,branch,family_table,fact_kind,format,start_revision);
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS family_table text NOT NULL DEFAULT '';
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS source_kind text NOT NULL DEFAULT '';
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS source_table text NOT NULL DEFAULT '';
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS value_digest bytea;
ALTER TABLE plug_postgres.selection_rows ADD COLUMN IF NOT EXISTS layout integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS selection_rows_family
 ON plug_postgres.selection_rows(incarnation,family_table,fact_kind,start_revision);

CREATE INDEX IF NOT EXISTS selection_rows_bead
 ON plug_postgres.selection_rows(incarnation,bead_key,start_revision)
 WHERE bead_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS selection_rows_source
 ON plug_postgres.selection_rows(incarnation,source_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_rows_priority
 ON plug_postgres.selection_rows(incarnation,priority,bead_id,start_revision)
 WHERE bead_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS selection_rows_type
 ON plug_postgres.selection_rows(incarnation,bead_type,bead_id,start_revision)
 WHERE bead_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS selection_rows_project
 ON plug_postgres.selection_rows(incarnation,project,bead_id,start_revision)
 WHERE bead_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS plug_postgres.selection_facts (
 incarnation text NOT NULL, row_key bytea NOT NULL,
 start_revision numeric(20,0) NOT NULL CHECK (start_revision BETWEEN 0 AND 18446744073709551615), ordinal integer NOT NULL,
 ref_key bytea, target_key bytea, subject bytea, scope bytea,
 scope_kind bytea, project_name bytea,
 role bytea, name bytea, value bytea, active boolean NOT NULL,
 depth bigint NOT NULL, link_kind bytea,
 PRIMARY KEY (incarnation,row_key,start_revision,ordinal),
 FOREIGN KEY (incarnation,row_key,start_revision)
 REFERENCES plug_postgres.selection_rows(incarnation,row_key,start_revision)
);
-- A separate fact kind certifies scoped metadata; adding columns never
-- upgrades generic grant rows, including complete empty generic domains.
ALTER TABLE plug_postgres.selection_facts ADD COLUMN IF NOT EXISTS scope_kind bytea;
ALTER TABLE plug_postgres.selection_facts ADD COLUMN IF NOT EXISTS project_name bytea;
CREATE INDEX IF NOT EXISTS selection_facts_ref
 ON plug_postgres.selection_facts(incarnation,row_key,ref_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_target
 ON plug_postgres.selection_facts(incarnation,row_key,target_key,ref_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_grant
 ON plug_postgres.selection_facts(incarnation,row_key,subject,scope,role,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_scoped_grant
 ON plug_postgres.selection_facts(incarnation,row_key,subject,scope_kind,project_name,role,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_name
 ON plug_postgres.selection_facts(incarnation,row_key,name,start_revision);

` + readyOrderSchema

// allocator_states is intentionally additive to schema version 1. initialize
// executes schemaDDL for every open, so an existing version-1 database gains
// the durable allocator table without rewriting any existing data.
