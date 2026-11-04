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
CREATE INDEX IF NOT EXISTS objects_kind_key ON objects(kind,key);
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

-- A virgin source lifetime is an O(1) admission marker. It records whether
-- any bead lifetime has ever been committed in this incarnation, so ordinary
-- commits never need to census every object just to reject first-publication
-- status.
CREATE TABLE IF NOT EXISTS selection_virgin_state (
 incarnation TEXT PRIMARY KEY,
 virgin INTEGER NOT NULL CHECK(virgin IN (0,1))
);

-- Additive representation only: an absent coverage row means unbuilt. Open
-- must not certify an existing store merely because these tables are empty.
CREATE TABLE IF NOT EXISTS hierarchy_index_coverage (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 format INTEGER NOT NULL CHECK(format=1),
 built_from TEXT COLLATE BINARY NOT NULL,
 certified_through TEXT COLLATE BINARY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('unbuilt','ready')),
 PRIMARY KEY(incarnation, branch),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(built_from)=20 AND built_from NOT GLOB '*[^0-9]*' AND built_from<='18446744073709551615'),
 CHECK(length(certified_through)=20 AND certified_through NOT GLOB '*[^0-9]*' AND certified_through<='18446744073709551615'),
 CHECK(built_from<=certified_through)
);
CREATE TABLE IF NOT EXISTS hierarchy_source_current (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 child_key TEXT COLLATE BINARY NOT NULL, parent_key TEXT COLLATE BINARY,
 ref BLOB NOT NULL, kind TEXT NOT NULL CHECK(kind='bead'),
 revision TEXT COLLATE BINARY NOT NULL, body BLOB,
 PRIMARY KEY(incarnation, branch, child_key),
 CHECK(parent_key IS NULL OR parent_key<>child_key),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision<='18446744073709551615')
);
CREATE INDEX IF NOT EXISTS hierarchy_source_current_parent
 ON hierarchy_source_current(incarnation, branch, parent_key, child_key)
 WHERE parent_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS hierarchy_source_history (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 child_key TEXT COLLATE BINARY NOT NULL, parent_key TEXT COLLATE BINARY,
 start_revision TEXT COLLATE BINARY NOT NULL, end_revision TEXT COLLATE BINARY,
 ref BLOB NOT NULL, kind TEXT NOT NULL CHECK(kind='bead'),
 revision TEXT COLLATE BINARY NOT NULL, body BLOB,
 PRIMARY KEY(incarnation, branch, child_key, start_revision),
 CHECK(parent_key IS NULL OR parent_key<>child_key),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision<='18446744073709551615'),
 CHECK(length(start_revision)=20 AND start_revision NOT GLOB '*[^0-9]*' AND start_revision<='18446744073709551615'),
 CHECK(end_revision IS NULL OR (length(end_revision)=20 AND end_revision NOT GLOB '*[^0-9]*' AND end_revision<='18446744073709551615' AND end_revision>start_revision)),
 CHECK(revision<=start_revision)
);
CREATE INDEX IF NOT EXISTS hierarchy_source_history_parent
 ON hierarchy_source_history(incarnation, branch, parent_key, child_key, start_revision)
 WHERE parent_key IS NOT NULL;

-- Outgoing-link coverage is independent of canonical Parent coverage. An
-- image row denotes a complete typed outgoing set, including the empty set.
-- Edge kinds are opaque bytes; custom values are never normalized as SQL text.
CREATE TABLE IF NOT EXISTS graph_link_index_coverage (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 format INTEGER NOT NULL CHECK(format=1),
 built_from TEXT COLLATE BINARY NOT NULL,
 certified_through TEXT COLLATE BINARY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('unbuilt','ready')),
 PRIMARY KEY(incarnation, branch),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(built_from)=20 AND built_from NOT GLOB '*[^0-9]*' AND built_from<='18446744073709551615'),
 CHECK(length(certified_through)=20 AND certified_through NOT GLOB '*[^0-9]*' AND certified_through<='18446744073709551615'),
 CHECK(built_from<=certified_through)
);
CREATE TABLE IF NOT EXISTS graph_link_source_current (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 source_key TEXT COLLATE BINARY NOT NULL, ref BLOB NOT NULL,
 kind TEXT NOT NULL CHECK(kind='bead'), revision TEXT COLLATE BINARY NOT NULL, body BLOB,
 PRIMARY KEY(incarnation, branch, source_key),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision<='18446744073709551615')
);
CREATE TABLE IF NOT EXISTS graph_link_current (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 source_key TEXT COLLATE BINARY NOT NULL, link_kind BLOB NOT NULL,
 target_key TEXT COLLATE BINARY NOT NULL,
 PRIMARY KEY(incarnation, branch, source_key, link_kind, target_key),
 CHECK(typeof(link_kind)='blob' AND length(link_kind) BETWEEN 1 AND 32),
 FOREIGN KEY(incarnation, branch, source_key)
  REFERENCES graph_link_source_current(incarnation, branch, source_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS graph_link_current_incoming
 ON graph_link_current(incarnation, branch, target_key, link_kind, source_key);
CREATE TABLE IF NOT EXISTS graph_link_source_history (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 source_key TEXT COLLATE BINARY NOT NULL,
 start_revision TEXT COLLATE BINARY NOT NULL, end_revision TEXT COLLATE BINARY,
 ref BLOB NOT NULL, kind TEXT NOT NULL CHECK(kind='bead'),
 revision TEXT COLLATE BINARY NOT NULL, body BLOB,
 PRIMARY KEY(incarnation, branch, source_key, start_revision),
 CHECK(length(branch)=20 AND branch NOT GLOB '*[^0-9]*' AND branch<='18446744073709551615'),
 CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision<='18446744073709551615'),
 CHECK(length(start_revision)=20 AND start_revision NOT GLOB '*[^0-9]*' AND start_revision<='18446744073709551615'),
 CHECK(end_revision IS NULL OR (length(end_revision)=20 AND end_revision NOT GLOB '*[^0-9]*' AND end_revision<='18446744073709551615' AND end_revision>start_revision)),
 CHECK(revision<=start_revision)
);
CREATE TABLE IF NOT EXISTS graph_link_history (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 source_key TEXT COLLATE BINARY NOT NULL, start_revision TEXT COLLATE BINARY NOT NULL,
 link_kind BLOB NOT NULL, target_key TEXT COLLATE BINARY NOT NULL,
 PRIMARY KEY(incarnation, branch, source_key, start_revision, link_kind, target_key),
 CHECK(typeof(link_kind)='blob' AND length(link_kind) BETWEEN 1 AND 32),
 FOREIGN KEY(incarnation, branch, source_key, start_revision)
  REFERENCES graph_link_source_history(incarnation, branch, source_key, start_revision) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS graph_link_history_incoming
 ON graph_link_history(incarnation, branch, target_key, link_kind, source_key, start_revision);

-- Legacy whole-selector coverage is deliberately not read by v2 admission.
-- Its single interval cannot prove independently complete source families.
CREATE TABLE IF NOT EXISTS selection_coverage (
 incarnation TEXT PRIMARY KEY, format INTEGER NOT NULL CHECK(format=2),
 built_from TEXT COLLATE BINARY NOT NULL,
 certified_through TEXT COLLATE BINARY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('ready','unbuilt')),
 CHECK(length(built_from)=20 AND length(certified_through)=20 AND built_from<=certified_through)
);
-- Each source-built family has its own closed temporal intervals. A later
-- unbuilt interval never erases a previously certified historical interval.
-- Existing populated stores gain no ready rows merely by opening this schema.
CREATE TABLE IF NOT EXISTS selection_family_coverage (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 family_table TEXT NOT NULL, family_kind TEXT NOT NULL,
 format INTEGER NOT NULL CHECK(format=2),
 built_from TEXT COLLATE BINARY NOT NULL,
 certified_through TEXT COLLATE BINARY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('ready','unbuilt')),
 PRIMARY KEY(incarnation,branch,family_table,family_kind,built_from),
 CHECK(length(branch)=20 AND length(built_from)=20 AND length(certified_through)=20),
 CHECK(built_from<=certified_through)
);
CREATE INDEX IF NOT EXISTS selection_family_coverage_lookup
 ON selection_family_coverage(incarnation,branch,family_table,family_kind,built_from DESC);
-- A build is never readable before its final coverage publication. Page rows
-- are tagged by this durable ledger so an interrupted build can discard only
-- its own physical projections in bounded fenced steps. Successful build
-- ledgers remain as provenance for their now-ordinary temporal rows.
CREATE TABLE IF NOT EXISTS selection_backfill_runs (
 run_id TEXT PRIMARY KEY, incarnation TEXT NOT NULL,
 at_revision TEXT COLLATE BINARY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('building','ready','aborted'))
);
CREATE TABLE IF NOT EXISTS selection_backfill_rows (
 run_id TEXT NOT NULL REFERENCES selection_backfill_runs(run_id),
 row_table TEXT NOT NULL CHECK(row_table IN ('bead','facts','source')),
 row_key TEXT COLLATE BINARY NOT NULL,
 PRIMARY KEY(run_id,row_table,row_key)
);
-- Thin temporal source observations let fact preflight validate revision,
-- presence and digest without reconstructing a record body or decoding a
-- complete derived row before the selector's SQL boundary. Rows are written
-- in the same Commit transaction as their source and selection projections.
CREATE TABLE IF NOT EXISTS selection_source_history (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 source_key TEXT COLLATE BINARY NOT NULL,
 start_revision TEXT COLLATE BINARY NOT NULL, end_revision TEXT COLLATE BINARY,
 source_revision TEXT COLLATE BINARY NOT NULL, source_present INTEGER NOT NULL,
 source_digest TEXT NOT NULL,
 PRIMARY KEY(incarnation,branch,source_key,start_revision),
 CHECK(source_present IN (0,1)),
 CHECK(end_revision IS NULL OR end_revision>start_revision)
);
CREATE INDEX IF NOT EXISTS selection_source_history_lookup
 ON selection_source_history(incarnation,branch,source_key,start_revision,end_revision);
CREATE INDEX IF NOT EXISTS selection_source_history_current
 ON selection_source_history(incarnation,branch,source_key) WHERE end_revision IS NULL;
CREATE TABLE IF NOT EXISTS selection_bead_history (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 derived_key TEXT COLLATE BINARY NOT NULL, record_key TEXT COLLATE BINARY NOT NULL,
 start_revision TEXT COLLATE BINARY NOT NULL, end_revision TEXT COLLATE BINARY,
 bead_id TEXT COLLATE BINARY NOT NULL, bead_type TEXT COLLATE BINARY NOT NULL,
 project TEXT COLLATE BINARY, status TEXT COLLATE BINARY,
 title TEXT COLLATE BINARY, template INTEGER,
 parent_key TEXT COLLATE BINARY, priority INTEGER,
 membership_leaf TEXT COLLATE BINARY NOT NULL,
 revision_text TEXT COLLATE BINARY NOT NULL,
 source_revision TEXT COLLATE BINARY NOT NULL, source_digest TEXT NOT NULL,
 PRIMARY KEY(incarnation, branch, derived_key, start_revision),
 CHECK(end_revision IS NULL OR end_revision>start_revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS selection_bead_current_record
 ON selection_bead_history(incarnation,branch,record_key) WHERE end_revision IS NULL;
-- Closing a changed logical row must not revisit its retained versions.
CREATE INDEX IF NOT EXISTS selection_bead_current_derived
 ON selection_bead_history(incarnation,branch,derived_key) WHERE end_revision IS NULL;
CREATE INDEX IF NOT EXISTS selection_bead_current_scope
 ON selection_bead_history(incarnation,branch,project,bead_type,bead_id)
 WHERE end_revision IS NULL;
CREATE INDEX IF NOT EXISTS selection_bead_history_record
 ON selection_bead_history(incarnation,branch,record_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_bead_history_interval
 ON selection_bead_history(incarnation,branch,start_revision,end_revision);

-- Complete fact-set envelopes exist even when there are zero fact rows. The
-- source binding is checked for every referenced set before any SQL boolean
-- branch, so NOT and false branches cannot turn missing coverage into truth.
CREATE TABLE IF NOT EXISTS selection_fact_sets (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 row_key TEXT COLLATE BINARY NOT NULL, row_table TEXT COLLATE BINARY NOT NULL,
 kind TEXT NOT NULL,
 start_revision TEXT COLLATE BINARY NOT NULL, end_revision TEXT COLLATE BINARY,
 source_key TEXT COLLATE BINARY NOT NULL,
 source_revision TEXT COLLATE BINARY NOT NULL, source_present INTEGER NOT NULL,
 source_digest TEXT NOT NULL,
 PRIMARY KEY(incarnation,branch,row_key,start_revision),
 CHECK(source_present IN (0,1)),
 CHECK(end_revision IS NULL OR end_revision>start_revision)
);
CREATE INDEX IF NOT EXISTS selection_fact_sets_interval
 ON selection_fact_sets(incarnation,branch,row_key,start_revision,end_revision);
CREATE INDEX IF NOT EXISTS selection_fact_sets_source
 ON selection_fact_sets(incarnation,branch,source_key)
 WHERE end_revision IS NULL;
-- Commit-local replacement checks address one physical row at a time. Keep
-- the row identity first so the lookup cannot fall back to scanning every
-- live bead source merely because it also carries a source-kind range.
CREATE INDEX IF NOT EXISTS selection_fact_sets_current_row
 ON selection_fact_sets(incarnation,branch,row_key,row_table,source_key,kind)
 WHERE end_revision IS NULL;
CREATE INDEX IF NOT EXISTS selection_fact_sets_family
 ON selection_fact_sets(incarnation,branch,row_table,kind,start_revision,end_revision,source_key);
-- Affected ancestor proofs join the temporal family image by source. Keep the
-- source identity before the revision range so a small affected set does not
-- walk every historical row in the family interval index.
CREATE INDEX IF NOT EXISTS selection_fact_sets_family_source
 ON selection_fact_sets(incarnation,branch,row_table,kind,source_key,start_revision,end_revision);
CREATE TABLE IF NOT EXISTS selection_facts (
 incarnation TEXT NOT NULL, branch TEXT COLLATE BINARY NOT NULL,
 row_key TEXT COLLATE BINARY NOT NULL, start_revision TEXT COLLATE BINARY NOT NULL,
 ordinal INTEGER NOT NULL, kind TEXT NOT NULL,
 ref_key TEXT COLLATE BINARY, target_key TEXT COLLATE BINARY,
 link_kind TEXT COLLATE BINARY, subject TEXT COLLATE BINARY NOT NULL,
 scope TEXT COLLATE BINARY NOT NULL, role TEXT COLLATE BINARY NOT NULL,
 -- scoped_grant_v1 alone populates these columns. NULL project_name is
 -- distinct from a present empty candidate project and cannot match it.
 scope_kind TEXT COLLATE BINARY, project_name TEXT COLLATE BINARY,
 name TEXT COLLATE BINARY NOT NULL, value TEXT COLLATE BINARY NOT NULL,
 active INTEGER NOT NULL, depth INTEGER NOT NULL,
 PRIMARY KEY(incarnation,branch,row_key,start_revision,ordinal),
 CHECK(active IN (0,1)),
 FOREIGN KEY(incarnation,branch,row_key,start_revision)
  REFERENCES selection_fact_sets(incarnation,branch,row_key,start_revision)
);
CREATE INDEX IF NOT EXISTS selection_facts_ref
 ON selection_facts(incarnation,branch,row_key,ref_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_target
 ON selection_facts(incarnation,branch,row_key,target_key,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_scope
 ON selection_facts(incarnation,branch,row_key,scope,start_revision);
CREATE INDEX IF NOT EXISTS selection_facts_scoped_project
 ON selection_facts(incarnation,branch,row_key,project_name,scope_kind,start_revision)
 WHERE kind='scoped_grant_v1';
`
