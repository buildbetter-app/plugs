package postgres

// The Ready proof has its own generation. It never mints or relaxes the
// independent Selection proof. Meta precedes BOTH fences on every watched
// statement, including the entire Selection-trigger footprint.
const readyOrderSchema = `
CREATE TABLE IF NOT EXISTS plug_postgres.ready_order_fence (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 generation numeric NOT NULL DEFAULT 0, certified_generation numeric,
 dirty boolean NOT NULL DEFAULT true, ever_source boolean NOT NULL DEFAULT false,
 format integer, incarnation text, head numeric(20,0), authority_head numeric(20,0),
 root_hash text,root_count numeric(20,0),eligibility_digest text,publication_digest text
);
CREATE TABLE IF NOT EXISTS plug_postgres.ready_order_changes (domain text NOT NULL,key bytea NOT NULL,PRIMARY KEY(domain,key));
-- Checkpoint overlays only extend a durable source Lease. That source already
-- makes the complete Ready domain Unsafe; an absent Lease ignores the overlay.
DROP TRIGGER IF EXISTS a_ready_order_mutated ON plug_postgres.checkpoints;
INSERT INTO plug_postgres.ready_order_fence(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION plug_postgres.ready_order_mutated() RETURNS trigger LANGUAGE plpgsql AS $ready$
BEGIN
 PERFORM singleton FROM plug_postgres.meta WHERE singleton FOR UPDATE;
 -- Retain that lock even when an already-dirty fence needs no new tuple.
 UPDATE plug_postgres.ready_order_fence SET
  generation=generation+CASE WHEN dirty THEN 0 ELSE 1 END,dirty=true,
  ever_source=ever_source OR TG_TABLE_NAME IN ('records','versions','states')
 WHERE singleton AND (NOT dirty OR (NOT ever_source AND TG_TABLE_NAME IN ('records','versions','states')));
 RETURN NULL;
END;
$ready$;
CREATE OR REPLACE FUNCTION plug_postgres.ready_order_changed_key() RETURNS trigger LANGUAGE plpgsql AS $ready$
BEGIN
 IF TG_OP<>'INSERT' THEN
  INSERT INTO plug_postgres.ready_order_changes(domain,key) VALUES(TG_ARGV[0],OLD.key) ON CONFLICT DO NOTHING;
 END IF;
 IF TG_OP<>'DELETE' THEN
  INSERT INTO plug_postgres.ready_order_changes(domain,key) VALUES(TG_ARGV[0],NEW.key) ON CONFLICT DO NOTHING;
 END IF;
 RETURN NULL;
END;
$ready$;
CREATE OR REPLACE FUNCTION plug_postgres.ready_order_nonempty_source() RETURNS trigger LANGUAGE plpgsql AS $ready$
BEGIN
 UPDATE plug_postgres.ready_order_fence SET ever_source=true WHERE singleton AND NOT ever_source;
 RETURN NULL;
END;
$ready$;
DO $ready$
DECLARE watched text;
BEGIN
 FOREACH watched IN ARRAY ARRAY['records','versions','states','core_sessions','hierarchy_index_coverage','hierarchy_source_current','hierarchy_source_history','graph_link_index_coverage','graph_link_source_current','graph_link_current','graph_link_source_history','graph_link_history','selection_coverage','selection_sources','selection_rows','selection_facts'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=('plug_postgres.'||watched)::regclass AND tgname='a_ready_order_mutated' AND NOT tgisinternal) THEN
   EXECUTE format('CREATE TRIGGER a_ready_order_mutated BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON plug_postgres.%I FOR EACH STATEMENT EXECUTE FUNCTION plug_postgres.ready_order_mutated()',watched);
  END IF;
 END LOOP;
 FOREACH watched IN ARRAY ARRAY['records','versions','states'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=('plug_postgres.'||watched)::regclass AND tgname='ready_order_changed_key' AND NOT tgisinternal) THEN
   EXECUTE format('CREATE TRIGGER ready_order_changed_key AFTER INSERT OR UPDATE OR DELETE ON plug_postgres.%I FOR EACH ROW EXECUTE FUNCTION plug_postgres.ready_order_changed_key(%L)',watched,CASE WHEN watched='states' THEN 'state' ELSE 'source' END);
  END IF;
 END LOOP;
 FOREACH watched IN ARRAY ARRAY['hierarchy_source_current','hierarchy_source_history','graph_link_source_current','graph_link_current','graph_link_source_history','graph_link_history','selection_sources','selection_rows','selection_facts'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=('plug_postgres.'||watched)::regclass AND tgname='ready_order_nonempty_source' AND NOT tgisinternal) THEN
   EXECUTE format('CREATE TRIGGER ready_order_nonempty_source AFTER INSERT OR UPDATE OR DELETE ON plug_postgres.%I FOR EACH ROW EXECUTE FUNCTION plug_postgres.ready_order_nonempty_source()',watched);
  END IF;
 END LOOP;
END;
$ready$;
`
