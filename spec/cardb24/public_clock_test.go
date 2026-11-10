// Clock/census helpers retained from landed main2902006b; prior test functions are not copied.
package cardb20

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// The existing fixture clock controls the durable driver (real SQL in
// PostgreSQL). This second clock controls application writes and stale reads.
// Keeping separate objects is essential: advancing only one must not silently
// move the other or change which clock owns receipt expiry.
func b24BindApplicationClock(t *testing.T, f *fixture, application *testclock.Clock) {
	t.Helper()
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(application.Now), pipeline.WithIDGenerator(f.nextID))
	f.service = service.New(f.store, f.lease, f.writer,
		service.WithOntology(f.ontology), service.WithClock(application.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	view, err := viewops.New(f.store, f.lease, viewops.Config{
		OntologyRevision: model.Natural64(1), StaleDefaultOntology: f.ontology, Now: application.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, view, viewrpc.Config{CursorKey: fixedCursorKey}); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{
		DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: fixedCursorKey, OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func b24StepClocks(t *testing.T, f *fixture, application *testclock.Clock, appDelta, durableDelta time.Duration) {
	t.Helper()
	// Reacquire after a durable step rather than extending the production lease.
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	application.Advance(t, appDelta)
	f.clock.Advance(t, durableDelta)
	f.bind(t)
	b24BindApplicationClock(t, f, application)
}

func b24ReopenClocks(t *testing.T, f *fixture, application *testclock.Clock) {
	t.Helper()
	if f.engine == "memory" {
		return // Memory is read back from the same store; no persistence claim.
	}
	previous := f.store
	f.reopen(t)
	if f.store == previous {
		t.Fatal("SQL reopen retained the previous store object")
	}
	b24BindApplicationClock(t, f, application)
}

type b24ReadImage struct {
	Logical  storedState
	Physical map[string][]string
}

func b24ReadSnapshot(t *testing.T, f *fixture) b24ReadImage {
	t.Helper()
	image := b24ReadImage{Logical: f.state(t)}
	if f.engine == "memory" {
		return image
	}
	name, dsn := "sqlite", f.path
	query := `SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if f.engine == "postgres" {
		name, dsn = "pgx", f.dsn
		query = `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename`
	}
	db, err := sql.Open(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatal("physical SQL census found no application tables")
	}
	image.Physical = make(map[string][]string, len(tables))
	for _, table := range tables {
		qualified := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		if f.engine == "postgres" {
			qualified = `plug_postgres.` + qualified
		}
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+qualified)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		image.Physical[table] = []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range pointers {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			row := make(map[string]any, len(columns))
			for i, column := range columns {
				row[column] = values[i]
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			image.Physical[table] = append(image.Physical[table], string(encoded))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(image.Physical[table])
	}
	return image
}

func b24ConservedRead(t *testing.T, f *fixture, phase string, operation func()) {
	t.Helper()
	before := b24ReadSnapshot(t, f)
	operation()
	after := b24ReadSnapshot(t, f)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("%s changed logical or complete physical state: before=%+v after=%+v", phase, before, after)
	}
	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("B24_READ_CONSERVATION engine=%s phase=%s physical_tables=%d image_sha256=%x", f.engine, phase, len(before.Physical), sha256.Sum256(encoded))
}

func b24ObserveClocks(t *testing.T, f *fixture, application *testclock.Clock, phase string) {
	t.Helper()
	app, durable := application.Now(), f.clock.Now()
	sqlTime := "not_applicable"
	if f.engine == "postgres" {
		observed := testclock.PostgresNow(t, f.dsn)
		if !observed.Equal(durable) {
			t.Fatalf("%s actual SQL=%s durable input=%s", phase, observed, durable)
		}
		sqlTime = observed.Format(time.RFC3339Nano)
	}
	t.Logf("B24_INDEPENDENT_CLOCK engine=%s phase=%s application=%s durable=%s sql=%s skew=%s native_dolt=UNQUALIFIED", f.engine, phase, app.Format(time.RFC3339Nano), durable.Format(time.RFC3339Nano), sqlTime, app.Sub(durable))
}

func b24StaleRead(t *testing.T, f *fixture, label string, want []string) {
	t.Helper()
	b24ConservedRead(t, f, label, func() {
		params := map[string]any{"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1}}
		var ids []string
		for page := 0; ; page++ {
			current, next := staleIDs(t, f.call("view.run", fmt.Sprintf("%s-page-%d", label, page), params))
			ids = append(ids, current...)
			if next == "" {
				break
			}
			if len(current) == 0 || page >= len(want) {
				t.Fatal("stale pagination did not advance within expected membership")
			}
			params["cursor"] = next
		}
		sort.Strings(ids)
		expected := append([]string(nil), want...)
		sort.Strings(expected)
		if !reflect.DeepEqual(ids, expected) {
			t.Fatalf("%s stale IDs=%v want=%v", label, ids, expected)
		}
		count := staleCount(t, f.call("view.run", label+"-count", map[string]any{
			"countOnly": true, "definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
		}))
		if count != len(want) {
			t.Fatalf("%s stale count=%d want=%d", label, count, len(want))
		}
	})
}
