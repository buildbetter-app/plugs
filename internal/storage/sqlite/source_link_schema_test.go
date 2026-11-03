package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSourceLinkSchemaIndependentCoverageAndPreIndexCatalog(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "source-links.db")
	s := openStore(t, ctx, path)
	inc, zero := getMeta(t, ctx, s).Incarnation, revKey(0)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO hierarchy_index_coverage VALUES(?,?,1,?,?,'ready')`, inc, zero, zero, zero); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"graph_link_index_coverage", "graph_link_source_current", "graph_link_current", "graph_link_source_history", "graph_link_history"} {
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("fresh %s count=%d err=%v", table, count, err)
		}
	}
	// Emulate a Parent-only catalog; its coverage must not initialize links.
	for _, table := range []string{"graph_link_current", "graph_link_history", "graph_link_source_current", "graph_link_source_history", "graph_link_index_coverage"} {
		if _, err := s.db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, ctx, path)
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM hierarchy_index_coverage WHERE incarnation=? AND branch=?`, inc, zero).Scan(&state); err != nil || state != "ready" {
		t.Fatalf("Parent marker was changed: state=%q err=%v", state, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM graph_link_index_coverage WHERE incarnation=? AND branch=?`, inc, zero).Scan(&state); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Parent-only coverage certified outgoing links: state=%q err=%v", state, err)
	}
	for index, want := range map[string][]string{
		"graph_link_current_incoming": {"incarnation", "branch", "target_key", "link_kind", "source_key"},
		"graph_link_history_incoming": {"incarnation", "branch", "target_key", "link_kind", "source_key", "start_revision"},
	} {
		rows, err := s.db.QueryContext(ctx, `SELECT name FROM pragma_index_info(?) ORDER BY seqno`, index)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				t.Fatal(err)
			}
			got = append(got, column)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s columns=%v want=%v", index, got, want)
		}
	}
}

func TestSourceLinkSchemaExactImagesIntervalsAndFactIntegrity(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "source-link-images.db"))
	inc, zero, high, maximum := getMeta(t, ctx, s).Incarnation, revKey(0), revKey(uint64(1)<<63), revKey(math.MaxUint64)
	key, _ := recordKey(testRef("pl-source"))
	target, _ := recordKey(testRef("pl-target"))
	body, kind := []byte(" exact opaque\x00\xff\n"), []byte("custom\x00kind")
	image := `INSERT INTO graph_link_source_current VALUES(?,?,?,?,'bead',?,?)`
	fact := `INSERT INTO graph_link_current VALUES(?,?,?,?,?)`
	if _, err := s.db.ExecContext(ctx, fact, inc, zero, key, kind, target); err == nil {
		t.Fatal("orphan incoming fact accepted")
	}
	if _, err := s.db.ExecContext(ctx, image, inc, zero, key, []byte("ref"), maximum, body); err != nil {
		t.Fatal(err)
	}
	var emptyCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_link_current WHERE source_key=?`, key).Scan(&emptyCount); err != nil || emptyCount != 0 {
		t.Fatalf("explicit empty set missing: count=%d err=%v", emptyCount, err)
	}
	if _, err := s.db.ExecContext(ctx, fact, inc, zero, key, kind, target); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, fact, inc, zero, key, kind, target); err == nil {
		t.Fatal("duplicate incoming fact accepted")
	}
	var exact, exactKind []byte
	if err := s.db.QueryRowContext(ctx, `SELECT s.body,e.link_kind FROM graph_link_current e JOIN graph_link_source_current s USING(incarnation,branch,source_key) WHERE e.target_key=?`, target).Scan(&exact, &exactKind); err != nil || !bytes.Equal(exact, body) || !bytes.Equal(exactKind, kind) {
		t.Fatalf("source/kind bytes changed: body=%q kind=%q err=%v", exact, exactKind, err)
	}
	historyImage := `INSERT INTO graph_link_source_history VALUES(?,?,?,?,?,?,'bead',?,?)`
	historyFact := `INSERT INTO graph_link_history VALUES(?,?,?,?,?,?)`
	if _, err := s.db.ExecContext(ctx, historyFact, inc, zero, key, high, []byte("blocks"), target); err == nil {
		t.Fatal("historical fact detached from exact source interval accepted")
	}
	if _, err := s.db.ExecContext(ctx, historyImage, inc, zero, key, high, maximum, []byte("ref"), high, body); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, historyFact, inc, zero, key, high, []byte("blocks"), target); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		at   string
		want int
	}{{zero, 0}, {high, 1}, {maximum, 0}} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_link_history e JOIN graph_link_source_history s USING(incarnation,branch,source_key,start_revision) WHERE e.target_key=? AND s.start_revision<=? AND (s.end_revision IS NULL OR s.end_revision>?)`, target, probe.at, probe.at).Scan(&count); err != nil || count != probe.want {
			t.Fatalf("historical interval at %s count=%d want=%d err=%v", probe.at, count, probe.want, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM graph_link_source_current WHERE source_key=?`, key); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_link_current`).Scan(&emptyCount); err != nil || emptyCount != 0 {
		t.Fatalf("delete retained incoming fact: count=%d err=%v", emptyCount, err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO graph_link_index_coverage VALUES(?,?,1,?,?,'ready')`, inc, zero, zero, maximum); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE graph_link_index_coverage SET certified_through='18446744073709551616'`); err == nil {
		t.Fatal("unsigned link coverage overflow accepted")
	}
}
