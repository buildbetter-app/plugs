package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestSourceLinkSchemaIndependentCoverageAndPreIndexCatalog(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inc := meta.Incarnation
	if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_index_coverage VALUES($1,0,1,0,0,'ready')`, inc); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"graph_link_index_coverage", "graph_link_source_current", "graph_link_current", "graph_link_source_history", "graph_link_history"} {
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM plug_postgres."+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("fresh %s count=%d err=%v", table, count, err)
		}
	}
	// A Parent-only catalog must reopen with independent outgoing coverage absent.
	for _, table := range []string{"graph_link_current", "graph_link_history", "graph_link_source_current", "graph_link_source_history", "graph_link_index_coverage"} {
		if _, err := s.db.ExecContext(ctx, "DROP TABLE plug_postgres."+table); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, dsn)
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM plug_postgres.hierarchy_index_coverage WHERE incarnation=$1 AND branch=0`, inc).Scan(&state); err != nil || state != "ready" {
		t.Fatalf("Parent marker was changed: state=%q err=%v", state, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM plug_postgres.graph_link_index_coverage WHERE incarnation=$1 AND branch=0`, inc).Scan(&state); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Parent-only coverage certified outgoing links: state=%q err=%v", state, err)
	}
	for index, columns := range map[string]string{
		"graph_link_current_incoming": "(incarnation, branch, target_key, link_kind, source_key)",
		"graph_link_history_incoming": "(incarnation, branch, target_key, link_kind, source_key, start_revision)",
	} {
		var definition string
		if err := s.db.QueryRowContext(ctx, `SELECT pg_get_indexdef($1::regclass)`, "plug_postgres."+index).Scan(&definition); err != nil || !strings.Contains(definition, "USING btree "+columns) {
			t.Fatalf("%s definition=%q err=%v", index, definition, err)
		}
	}
}

func TestSourceLinkSchemaExactImagesIntervalsAndFactIntegrity(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inc := meta.Incarnation
	const zero, high, maximum = "0", "9223372036854775808", "18446744073709551615"
	key, _ := recordKey(testRef("pl-source"))
	target, _ := recordKey(testRef("pl-target"))
	body, kind := []byte(" exact opaque\x00\xff\n"), []byte("custom\x00kind")
	image := `INSERT INTO plug_postgres.graph_link_source_current VALUES($1,0,$2,$3,'bead',$4,$5)`
	fact := `INSERT INTO plug_postgres.graph_link_current VALUES($1,0,$2,$3,$4)`
	if _, err := s.db.ExecContext(ctx, fact, inc, []byte(key), kind, []byte(target)); err == nil {
		t.Fatal("orphan incoming fact accepted")
	}
	if _, err := s.db.ExecContext(ctx, image, inc, []byte(key), []byte("ref"), maximum, body); err != nil {
		t.Fatal(err)
	}
	var emptyCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plug_postgres.graph_link_current WHERE source_key=$1`, []byte(key)).Scan(&emptyCount); err != nil || emptyCount != 0 {
		t.Fatalf("explicit empty set missing: count=%d err=%v", emptyCount, err)
	}
	if _, err := s.db.ExecContext(ctx, fact, inc, []byte(key), kind, []byte(target)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, fact, inc, []byte(key), kind, []byte(target)); err == nil {
		t.Fatal("duplicate incoming fact accepted")
	}
	var exact, exactKind []byte
	if err := s.db.QueryRowContext(ctx, `SELECT s.body,e.link_kind FROM plug_postgres.graph_link_current e JOIN plug_postgres.graph_link_source_current s USING(incarnation,branch,source_key) WHERE e.target_key=$1`, []byte(target)).Scan(&exact, &exactKind); err != nil || !bytes.Equal(exact, body) || !bytes.Equal(exactKind, kind) {
		t.Fatalf("source/kind bytes changed: body=%q kind=%q err=%v", exact, exactKind, err)
	}
	historyImage := `INSERT INTO plug_postgres.graph_link_source_history VALUES($1,0,$2,$3,$4,$5,'bead',$3,$6)`
	historyFact := `INSERT INTO plug_postgres.graph_link_history VALUES($1,0,$2,$3,$4,$5)`
	if _, err := s.db.ExecContext(ctx, historyFact, inc, []byte(key), high, []byte("blocks"), []byte(target)); err == nil {
		t.Fatal("historical fact detached from exact source interval accepted")
	}
	if _, err := s.db.ExecContext(ctx, historyImage, inc, []byte(key), high, maximum, []byte("ref"), body); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, historyFact, inc, []byte(key), high, []byte("blocks"), []byte(target)); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		at   string
		want int
	}{{zero, 0}, {high, 1}, {maximum, 0}} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plug_postgres.graph_link_history e JOIN plug_postgres.graph_link_source_history s USING(incarnation,branch,source_key,start_revision) WHERE e.target_key=$1 AND s.start_revision<=$2 AND (s.end_revision IS NULL OR s.end_revision>$2)`, []byte(target), probe.at).Scan(&count); err != nil || count != probe.want {
			t.Fatalf("historical interval at %s count=%d want=%d err=%v", probe.at, count, probe.want, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM plug_postgres.graph_link_source_current WHERE source_key=$1`, []byte(key)); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plug_postgres.graph_link_current`).Scan(&emptyCount); err != nil || emptyCount != 0 {
		t.Fatalf("delete retained incoming fact: count=%d err=%v", emptyCount, err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.graph_link_index_coverage VALUES($1,0,1,0,$2,'ready')`, inc, maximum); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.graph_link_index_coverage SET certified_through=18446744073709551616`); err == nil {
		t.Fatal("unsigned link coverage overflow accepted")
	}
}
