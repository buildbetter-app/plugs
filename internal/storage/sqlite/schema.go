package sqlite

const schemaVersion = 3

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
-- Query read-work counters are maintained transactionally so a page does not
-- need to scan the full objects/records corpus merely to preserve the
-- historical discarded-candidate accounting contract.
CREATE TABLE IF NOT EXISTS query_read_work (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 selector_keys INTEGER NOT NULL,
 payloads INTEGER NOT NULL,
 payload_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
 key TEXT PRIMARY KEY,
 kind TEXT NOT NULL,
 -- Dense lexical rank of live records. Query cursors carry this indexed sort
 -- value, so the read path remains stateless and native restore keeps its
 -- existing monotonically advancing numeric cursor contract.
 query_order INTEGER NOT NULL DEFAULT 0,
 query_kind_order INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS objects_query_order_key ON objects(query_order, key);
CREATE INDEX IF NOT EXISTS objects_kind_query_order_key ON objects(kind, query_kind_order, key);
-- Kind-filtered keyset pages order by the opaque record key. Keep this index
-- separate from the dense rank witness used by native restore.
CREATE INDEX IF NOT EXISTS objects_kind_key ON objects(kind, key);
CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TRIGGER IF NOT EXISTS query_read_work_objects_insert
AFTER INSERT ON objects
BEGIN
 UPDATE query_read_work SET selector_keys=selector_keys+1 WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS query_read_work_records_insert
AFTER INSERT ON records
BEGIN
 UPDATE query_read_work
 SET payloads=payloads+1,
     payload_bytes=payload_bytes+
       CASE WHEN json_valid(NEW.data) AND json_extract(NEW.data, '$.Body') IS NOT NULL
            THEN (length(json_extract(NEW.data, '$.Body'))/4)*3
              - CASE WHEN substr(json_extract(NEW.data, '$.Body'), -1)='=' THEN 1 ELSE 0 END
              - CASE WHEN substr(json_extract(NEW.data, '$.Body'), -2, 1)='=' THEN 1 ELSE 0 END
            ELSE 0 END
 WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS query_read_work_records_update
AFTER UPDATE OF data ON records
BEGIN
 UPDATE query_read_work
 SET payload_bytes=payload_bytes
       + CASE WHEN json_valid(NEW.data) AND json_extract(NEW.data, '$.Body') IS NOT NULL
              THEN (length(json_extract(NEW.data, '$.Body'))/4)*3
                - CASE WHEN substr(json_extract(NEW.data, '$.Body'), -1)='=' THEN 1 ELSE 0 END
                - CASE WHEN substr(json_extract(NEW.data, '$.Body'), -2, 1)='=' THEN 1 ELSE 0 END
              ELSE 0 END
       - CASE WHEN json_valid(OLD.data) AND json_extract(OLD.data, '$.Body') IS NOT NULL
              THEN (length(json_extract(OLD.data, '$.Body'))/4)*3
                - CASE WHEN substr(json_extract(OLD.data, '$.Body'), -1)='=' THEN 1 ELSE 0 END
                - CASE WHEN substr(json_extract(OLD.data, '$.Body'), -2, 1)='=' THEN 1 ELSE 0 END
              ELSE 0 END
 WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS query_read_work_records_delete
AFTER DELETE ON records
BEGIN
 UPDATE query_read_work
 SET payloads=payloads-1,
     payload_bytes=payload_bytes-
       CASE WHEN json_valid(OLD.data) AND json_extract(OLD.data, '$.Body') IS NOT NULL
            THEN (length(json_extract(OLD.data, '$.Body'))/4)*3
              - CASE WHEN substr(json_extract(OLD.data, '$.Body'), -1)='=' THEN 1 ELSE 0 END
              - CASE WHEN substr(json_extract(OLD.data, '$.Body'), -2, 1)='=' THEN 1 ELSE 0 END
            ELSE 0 END
 WHERE singleton=1;
END;
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
`
