package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
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
			ctx := context.Background()
			dsn := freshDSN(t)
			s := openTest(t, dsn)
			body := []byte("{ \"parent\" : null, \"opaque\" : [ 1, 2 ] }\n")
			if preIndex {
				lease := testLease(t, s)
				if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("pl-legacy"), Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
					t.Fatal(err)
				}
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				// Disposable pre-index catalog with real durable legacy source,
				// versions and log. This does not claim an old-binary run.
				for _, table := range []string{"hierarchy_source_current", "hierarchy_source_history", "hierarchy_index_coverage"} {
					if _, err := s.db.ExecContext(ctx, "DROP TABLE plug_postgres."+table); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openTest(t, dsn)
			if after, err := s.Meta(ctx); err != nil || after != before {
				t.Fatalf("additive open rewrote metadata: before=%+v after=%+v err=%v", before, after, err)
			}
			for _, table := range []string{"hierarchy_index_coverage", "hierarchy_source_current", "hierarchy_source_history"} {
				var count int
				if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM plug_postgres."+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s was not installed empty: count=%d err=%v", table, count, err)
				}
			}
			var state string
			err = s.db.QueryRowContext(ctx, `SELECT state FROM plug_postgres.hierarchy_index_coverage WHERE incarnation=$1 AND branch=0`, before.Incarnation).Scan(&state)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("open certified an unbuilt range: state=%s err=%v", state, err)
			}
			for index, want := range map[string]string{
				"hierarchy_source_current_parent": "(incarnation, branch, parent_key, child_key)",
				"hierarchy_source_history_parent": "(incarnation, branch, parent_key, child_key, start_revision)",
			} {
				var definition string
				if err := s.db.QueryRowContext(ctx, `SELECT pg_get_indexdef($1::regclass)`, "plug_postgres."+index).Scan(&definition); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(definition, "USING btree "+want) || !strings.Contains(definition, "WHERE (parent_key IS NOT NULL)") {
					t.Fatalf("catalog %s definition=%q", index, definition)
				}
			}
			if preIndex {
				r := testRead(t, s, nil)
				records, err := r.Get(ctx, []driver.Ref{testRef("pl-legacy")})
				if err != nil || len(records) != 1 || !bytes.Equal(records[0].Body, body) {
					t.Fatalf("legacy exact image after schema install: records=%+v err=%v", records, err)
				}
			}
		})
	}
}

func TestHierarchySchemaUnsignedIntervalsExactBytesAndConstraints(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inc := meta.Incarnation
	const zero, high, maximum = "0", "9223372036854775808", "18446744073709551615"
	if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_index_coverage VALUES($1,0,1,$2,$3,'ready')`, inc, zero, maximum); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"-1", "18446744073709551616"} {
		if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.hierarchy_index_coverage SET certified_through=$1`, bad); err == nil {
			t.Fatalf("out-of-domain unsigned revision %q accepted", bad)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.hierarchy_index_coverage SET built_from=$1,certified_through=$2`, maximum, high); err == nil {
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
		if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_source_current VALUES($1,0,$2,$3,$4,'bead',$5,$6)`, inc, []byte(key), []byte(parent), []byte(id), maximum, body); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keys)
	rows, err := s.db.QueryContext(ctx, `SELECT child_key,revision::text,body FROM plug_postgres.hierarchy_source_current WHERE incarnation=$1 AND branch=0 AND parent_key=$2 ORDER BY child_key LIMIT 3`, inc, []byte(parent))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var key, exact []byte
		var revision string
		if err := rows.Scan(&key, &revision, &exact); err != nil {
			t.Fatal(err)
		}
		if revision != maximum || !bytes.Equal(exact, body) {
			t.Fatalf("source image changed: revision=%q body=%q", revision, exact)
		}
		got = append(got, string(key))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("canonical keys conflated or misordered: %q want=%q", got, keys)
	}
	insert := `INSERT INTO plug_postgres.hierarchy_source_current VALUES($1,0,$2,$3,$4,'bead',$5,$6)`
	if _, err := s.db.ExecContext(ctx, insert, inc, []byte(keys[0]), []byte(other), []byte("duplicate"), zero, body); err == nil {
		t.Fatal("second parent for one canonical child accepted")
	}
	if _, err := s.db.ExecContext(ctx, insert, inc, []byte("self"), []byte("self"), []byte("self"), zero, body); err == nil {
		t.Fatal("self parent accepted")
	}
	if _, err := s.db.ExecContext(ctx, insert, inc, []byte("root"), nil, []byte("root"), zero, nil); err != nil {
		t.Fatal(err)
	}
	for _, interval := range []struct {
		start string
		end   any
		p     []byte
	}{{zero, high, []byte(parent)}, {high, maximum, []byte(other)}, {maximum, nil, nil}} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_source_history VALUES($1,0,$2,$3,$4,$5,$6,'bead',$4,$7)`, inc, []byte(keys[0]), interval.p, interval.start, interval.end, []byte("ref"), body); err != nil {
			t.Fatal(err)
		}
	}
	for _, probe := range []struct {
		at, parent string
	}{{zero, parent}, {high, other}, {maximum, ""}} {
		var p, exact []byte
		err := s.db.QueryRowContext(ctx, `SELECT parent_key,body FROM plug_postgres.hierarchy_source_history WHERE incarnation=$1 AND branch=0 AND child_key=$2 AND start_revision<=$3 AND (end_revision IS NULL OR end_revision>$3)`, inc, []byte(keys[0]), probe.at).Scan(&p, &exact)
		if err != nil || string(p) != probe.parent || (p == nil) != (probe.parent == "") || !bytes.Equal(exact, body) {
			t.Fatalf("as-of %s: parent=%q body=%q err=%v", probe.at, p, exact, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_source_history VALUES($1,0,$2,$3,$4,$5,$6,'bead',$4,$7)`, inc, []byte("bad-interval"), []byte(parent), high, zero, []byte("ref"), body); err == nil {
		t.Fatal("empty or reversed historical interval accepted")
	}
}
