package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestHierarchySchemaFreshAndPreIndexReopenRemainUnbuilt(t *testing.T) {
	for _, preIndex := range []bool{false, true} {
		name := "fresh"
		if preIndex {
			name = "pre-index"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "hierarchy.db")
			s := openStore(t, ctx, path)
			body := []byte("{ \"parent\" : null, \"opaque\" : [ 1, 2 ] }\n")
			if preIndex {
				lease := getLease(t, ctx, s, driver.LeaseTTL)
				if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("pl-legacy"), Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
					t.Fatal(err)
				}
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				// Make a disposable pre-index catalog around real durable legacy
				// records/history/log. No old binary or synthetic history is claimed.
				for _, table := range []string{"hierarchy_source_current", "hierarchy_source_history", "hierarchy_index_coverage"} {
					if _, err := s.db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := getMeta(t, ctx, s)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, ctx, path)
			if after := getMeta(t, ctx, s); after != before {
				t.Fatalf("additive open rewrote metadata: before=%+v after=%+v", before, after)
			}
			for _, table := range []string{"hierarchy_index_coverage", "hierarchy_source_current", "hierarchy_source_history"} {
				var count int
				if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s was not installed empty: count=%d err=%v", table, count, err)
				}
			}
			var state string
			err := s.db.QueryRowContext(ctx, `SELECT state FROM hierarchy_index_coverage WHERE incarnation=? AND branch=?`, before.Incarnation, revKey(0)).Scan(&state)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("open certified an unbuilt range: state=%s err=%v", state, err)
			}
			for index, want := range map[string][]string{
				"hierarchy_source_current_parent": {"incarnation", "branch", "parent_key", "child_key"},
				"hierarchy_source_history_parent": {"incarnation", "branch", "parent_key", "child_key", "start_revision"},
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
					t.Fatalf("catalog %s columns=%v want=%v", index, got, want)
				}
			}
			if preIndex {
				r := getReader(t, ctx, s, before, nil)
				records, err := r.Get(ctx, []driver.Ref{testRef("pl-legacy")})
				if err != nil || len(records) != 1 || !bytes.Equal(records[0].Body, body) {
					t.Fatalf("legacy exact image after schema install: records=%+v err=%v", records, err)
				}
			}
		})
	}
}

func TestHierarchySchemaUnsignedIntervalsExactBytesAndConstraints(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "hierarchy-domain.db"))
	inc := getMeta(t, ctx, s).Incarnation
	zero, high, maximum := revKey(0), revKey(uint64(1)<<63), revKey(math.MaxUint64)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO hierarchy_index_coverage VALUES(?,?,1,?,?,'ready')`, inc, zero, zero, maximum); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"1", "-0000000000000000001", "18446744073709551616", "0000000000000000000x"} {
		if _, err := s.db.ExecContext(ctx, `UPDATE hierarchy_index_coverage SET certified_through=?`, bad); err == nil {
			t.Fatalf("noncanonical unsigned revision %q accepted", bad)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE hierarchy_index_coverage SET built_from=?,certified_through=?`, maximum, high); err == nil {
		t.Fatal("reversed coverage range accepted")
	}
	parent, _ := recordKey(testRef("pl-parent:one/two"))
	other, _ := recordKey(testRef("pl-other"))
	keys := make([]string, 0, 3)
	body := []byte(" opaque\x00\xff bytes\n")
	for _, id := range []string{"pl-z", "pl-a:part/one", "pl-a/part:one"} {
		key, err := recordKey(testRef(id))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
		if _, err := s.db.ExecContext(ctx, `INSERT INTO hierarchy_source_current VALUES(?,?,?,?,?,'bead',?,?)`, inc, zero, key, parent, []byte(id), maximum, body); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keys)
	rows, err := s.db.QueryContext(ctx, `SELECT child_key,revision,body FROM hierarchy_source_current WHERE incarnation=? AND branch=? AND parent_key=? ORDER BY child_key LIMIT 3`, inc, zero, parent)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var key, revision string
		var exact []byte
		if err := rows.Scan(&key, &revision, &exact); err != nil {
			t.Fatal(err)
		}
		if revision != maximum || !bytes.Equal(exact, body) {
			t.Fatalf("source image changed: revision=%q body=%q", revision, exact)
		}
		got = append(got, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("canonical keys conflated or misordered: %q want=%q", got, keys)
	}
	insert := `INSERT INTO hierarchy_source_current VALUES(?,?,?,?,?,'bead',?,?)`
	if _, err := s.db.ExecContext(ctx, insert, inc, zero, keys[0], other, []byte("duplicate"), zero, body); err == nil {
		t.Fatal("second parent for one canonical child accepted")
	}
	if _, err := s.db.ExecContext(ctx, insert, inc, zero, "self", "self", []byte("self"), zero, body); err == nil {
		t.Fatal("self parent accepted")
	}
	if _, err := s.db.ExecContext(ctx, insert, inc, zero, "root", nil, []byte("root"), zero, nil); err != nil {
		t.Fatal(err)
	}
	for _, interval := range []struct {
		start string
		end   any
		p     any
	}{{zero, high, parent}, {high, maximum, other}, {maximum, nil, nil}} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO hierarchy_source_history VALUES(?,?,?,?,?,?,?,'bead',?,?)`, inc, zero, keys[0], interval.p, interval.start, interval.end, []byte("ref"), interval.start, body); err != nil {
			t.Fatal(err)
		}
	}
	for _, probe := range []struct {
		at, parent string
	}{{zero, parent}, {high, other}, {maximum, ""}} {
		var p sql.NullString
		var exact []byte
		if err := s.db.QueryRowContext(ctx, `SELECT parent_key,body FROM hierarchy_source_history WHERE incarnation=? AND branch=? AND child_key=? AND start_revision<=? AND (end_revision IS NULL OR end_revision>?)`, inc, zero, keys[0], probe.at, probe.at).Scan(&p, &exact); err != nil || p.String != probe.parent || p.Valid != (probe.parent != "") || !bytes.Equal(exact, body) {
			t.Fatalf("as-of %s: parent=%+v body=%q err=%v", probe.at, p, exact, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO hierarchy_source_history VALUES(?,?,?,?,?,?,?,'bead',?,?)`, inc, zero, "bad-interval", parent, high, zero, []byte("ref"), high, body); err == nil {
		t.Fatal("empty or reversed historical interval accepted")
	}
}
