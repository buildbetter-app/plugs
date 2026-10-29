package auditsql_test

import (
	"context"
	"testing"
)

// Faults live in the disposable engine, outside the implementation. No fake
// store, transaction, decision, descriptor, or checkpoint adapter is involved.
func (e engine) checkpointFault(t *testing.T, ctx context.Context, install bool) {
	t.Helper()
	statement := `DROP TRIGGER audit_checkpoint_fault`
	if install {
		statement = `CREATE TRIGGER audit_checkpoint_fault BEFORE UPDATE ON checkpoints
WHEN json_extract(CAST(NEW.payload AS TEXT),'$.position.index')=1
BEGIN SELECT RAISE(ABORT,'audit checkpoint failure'); END`
	}
	if e.name == "postgres" {
		statement = `DROP TRIGGER audit_checkpoint_fault ON plug_postgres.checkpoints; DROP FUNCTION plug_postgres.audit_checkpoint_fault()`
		if install {
			statement = `CREATE FUNCTION plug_postgres.audit_checkpoint_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'audit checkpoint failure'; END$$;
CREATE TRIGGER audit_checkpoint_fault BEFORE UPDATE ON plug_postgres.checkpoints FOR EACH ROW
WHEN ((convert_from(NEW.payload,'UTF8')::jsonb->'position'->>'index')='1')
EXECUTE FUNCTION plug_postgres.audit_checkpoint_fault()`
		}
	}
	e.exec(t, ctx, statement)
}

func (e engine) receiptFault(t *testing.T, ctx context.Context, install bool) {
	t.Helper()
	statement := `DROP TRIGGER audit_receipt_fault`
	if install {
		statement = `CREATE TRIGGER audit_receipt_fault BEFORE INSERT ON replays BEGIN
SELECT CASE WHEN
 (SELECT json_array_length(CAST(data AS TEXT),'$.events') FROM changesets ORDER BY rev DESC LIMIT 1)=3
 AND (SELECT json_extract(CAST(data AS TEXT),'$.events[0].type') FROM changesets ORDER BY rev DESC LIMIT 1)='bead.closed'
 THEN RAISE(ABORT,'audit late receipt failure')
 ELSE RAISE(ABORT,'audit plan missing before receipt') END; END`
	}
	if e.name == "postgres" {
		statement = `DROP TRIGGER audit_receipt_fault ON plug_postgres.receipts; DROP FUNCTION plug_postgres.audit_receipt_fault()`
		if install {
			statement = `CREATE FUNCTION plug_postgres.audit_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE plan jsonb;
BEGIN
 SELECT convert_from(value,'UTF8')::jsonb INTO plan FROM plug_postgres.changesets ORDER BY revision DESC LIMIT 1;
 IF jsonb_array_length(plan->'events')=3 AND plan->'events'->0->>'type'='bead.closed' THEN
  RAISE EXCEPTION 'audit late receipt failure';
 ELSE RAISE EXCEPTION 'audit plan missing before receipt'; END IF;
END$$;
CREATE TRIGGER audit_receipt_fault BEFORE INSERT ON plug_postgres.receipts FOR EACH ROW EXECUTE FUNCTION plug_postgres.audit_receipt_fault()`
		}
	}
	e.exec(t, ctx, statement)
}

func (e engine) familyCounts(t *testing.T, ctx context.Context) map[string]int {
	t.Helper()
	tables := []string{"objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints"}
	if e.name == "postgres" {
		// PostgreSQL lock placeholders are explicitly unversioned coordination
		// rows. All families that hold graph/history/receipt payloads are here.
		tables = []string{"plug_postgres.records", "plug_postgres.versions", "plug_postgres.states", "plug_postgres.blobs", "plug_postgres.changesets", "plug_postgres.receipts", "plug_postgres.checkpoints"}
	}
	db := e.db(t)
	defer db.Close()
	counts := map[string]int{}
	for _, table := range tables {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}
