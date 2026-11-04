package sqlite

// A private proof lifetime. Existing populated stores start unproved. A clean
// generation is minted only after the engine verifies a complete typed delta.
// Triggers cover source bytes/history, opaque Ready+authority rows, physical
// source/authority projections, and fixed-Head backfill/restore bypasses.
const readyOrderSchema = `
CREATE TABLE IF NOT EXISTS ready_order_fence (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 generation TEXT NOT NULL, certified_generation TEXT,
 dirty INTEGER NOT NULL CHECK(dirty IN (0,1)),
 ever_source INTEGER NOT NULL CHECK(ever_source IN (0,1)),
 format INTEGER, incarnation TEXT, head TEXT, authority_head TEXT,
 root_hash TEXT, root_count TEXT, eligibility_digest TEXT, publication_digest TEXT
);
CREATE TABLE IF NOT EXISTS ready_order_changes (domain TEXT NOT NULL,key TEXT NOT NULL,PRIMARY KEY(domain,key));
-- Quiet heartbeat checkpoints cannot introduce a durable claim. EffectiveLease
-- ignores them when the source Lease is nil; a source Lease makes Ready Unsafe.
-- Removing a claim is a maintained source mutation and may restore coverage.
DROP TRIGGER IF EXISTS ready_order_checkpoints_insert;
DROP TRIGGER IF EXISTS ready_order_checkpoints_update;
DROP TRIGGER IF EXISTS ready_order_checkpoints_delete;
-- Replace prototype journal triggers as part of the same schema transaction.
DROP TRIGGER IF EXISTS ready_order_records_insert;
DROP TRIGGER IF EXISTS ready_order_records_update;
DROP TRIGGER IF EXISTS ready_order_records_delete;
DROP TRIGGER IF EXISTS ready_order_versions_insert;
DROP TRIGGER IF EXISTS ready_order_versions_update;
DROP TRIGGER IF EXISTS ready_order_versions_delete;
DROP TRIGGER IF EXISTS ready_order_key_history_insert;
DROP TRIGGER IF EXISTS ready_order_key_history_update;
DROP TRIGGER IF EXISTS ready_order_key_history_delete;
INSERT OR IGNORE INTO ready_order_fence(singleton,generation,dirty,ever_source) VALUES(1,'unproved',1,0);
-- The outer source statement may use OR REPLACE. Avoid journal conflicts
-- explicitly so its conflict policy cannot override a trigger OR IGNORE.
CREATE TRIGGER IF NOT EXISTS ready_order_records_insert BEFORE INSERT ON records BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_records_update BEFORE UPDATE ON records BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=OLD.key); INSERT INTO ready_order_changes(domain,key) SELECT 'source',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_records_delete BEFORE DELETE ON records BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=OLD.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_versions_insert BEFORE INSERT ON versions BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_versions_update BEFORE UPDATE ON versions BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=OLD.key); INSERT INTO ready_order_changes(domain,key) SELECT 'source',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_versions_delete BEFORE DELETE ON versions BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'source',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='source' AND key=OLD.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_key_history_insert BEFORE INSERT ON key_history BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'state',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='state' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_key_history_update BEFORE UPDATE ON key_history BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'state',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='state' AND key=OLD.key); INSERT INTO ready_order_changes(domain,key) SELECT 'state',NEW.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='state' AND key=NEW.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_key_history_delete BEFORE DELETE ON key_history BEGIN
 INSERT INTO ready_order_changes(domain,key) SELECT 'state',OLD.key WHERE NOT EXISTS(SELECT 1 FROM ready_order_changes WHERE domain='state' AND key=OLD.key); UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_objects_insert BEFORE INSERT ON objects BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_objects_update BEFORE UPDATE ON objects BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_objects_delete BEFORE DELETE ON objects BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_core_sessions_insert BEFORE INSERT ON core_sessions BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_core_sessions_update BEFORE UPDATE ON core_sessions BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_core_sessions_delete BEFORE DELETE ON core_sessions BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_index_coverage_insert BEFORE INSERT ON hierarchy_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_index_coverage_update BEFORE UPDATE ON hierarchy_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_index_coverage_delete BEFORE DELETE ON hierarchy_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_current_insert BEFORE INSERT ON hierarchy_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_current_update BEFORE UPDATE ON hierarchy_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_current_delete BEFORE DELETE ON hierarchy_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_history_insert BEFORE INSERT ON hierarchy_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_history_update BEFORE UPDATE ON hierarchy_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_hierarchy_source_history_delete BEFORE DELETE ON hierarchy_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_index_coverage_insert BEFORE INSERT ON graph_link_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_index_coverage_update BEFORE UPDATE ON graph_link_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_index_coverage_delete BEFORE DELETE ON graph_link_index_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_current_insert BEFORE INSERT ON graph_link_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_current_update BEFORE UPDATE ON graph_link_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_current_delete BEFORE DELETE ON graph_link_source_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_current_insert BEFORE INSERT ON graph_link_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_current_update BEFORE UPDATE ON graph_link_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_current_delete BEFORE DELETE ON graph_link_current BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_history_insert BEFORE INSERT ON graph_link_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_history_update BEFORE UPDATE ON graph_link_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_source_history_delete BEFORE DELETE ON graph_link_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_history_insert BEFORE INSERT ON graph_link_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_history_update BEFORE UPDATE ON graph_link_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_graph_link_history_delete BEFORE DELETE ON graph_link_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_coverage_insert BEFORE INSERT ON selection_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_coverage_update BEFORE UPDATE ON selection_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_coverage_delete BEFORE DELETE ON selection_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_family_coverage_insert BEFORE INSERT ON selection_family_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_family_coverage_update BEFORE UPDATE ON selection_family_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_family_coverage_delete BEFORE DELETE ON selection_family_coverage BEGIN
 UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_source_history_insert BEFORE INSERT ON selection_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_source_history_update BEFORE UPDATE ON selection_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_source_history_delete BEFORE DELETE ON selection_source_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_bead_history_insert BEFORE INSERT ON selection_bead_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_bead_history_update BEFORE UPDATE ON selection_bead_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_bead_history_delete BEFORE DELETE ON selection_bead_history BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_fact_sets_insert BEFORE INSERT ON selection_fact_sets BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_fact_sets_update BEFORE UPDATE ON selection_fact_sets BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_fact_sets_delete BEFORE DELETE ON selection_fact_sets BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_facts_insert BEFORE INSERT ON selection_facts BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_facts_update BEFORE UPDATE ON selection_facts BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
CREATE TRIGGER IF NOT EXISTS ready_order_selection_facts_delete BEFORE DELETE ON selection_facts BEGIN
 UPDATE ready_order_fence SET ever_source=1 WHERE singleton=1 AND ever_source=0; UPDATE ready_order_fence SET generation=lower(hex(randomblob(32))),dirty=1 WHERE singleton=1 AND dirty=0;
END;
`
