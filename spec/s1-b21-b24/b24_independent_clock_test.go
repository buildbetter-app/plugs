package s1b21b24

import (
	"bytes"
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

func TestS1B24IndependentClockStaleBoundaryAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.ids = []string{"b24-clock-first", "b24-clock-second", "b24-clock-newer"}
			application := testclock.New(t, f.clock.Now())
			b24BindApplicationClock(t, f, application)
			tick := time.Nanosecond
			if engine == "postgres" {
				tick = time.Microsecond
			}
			first, _ := f.create(t, "independent-stale-first", "first")
			second, _ := f.create(t, "independent-stale-second", "second")
			origin := application.Now()
			b24StepClocks(t, f, application, 2*tick, 0)
			newer, _ := f.create(t, "independent-stale-newer", "newer")
			state := f.state(t)
			if !latestVersion(t, state, first).CreatedAt.Equal(origin) || !latestVersion(t, state, newer).CreatedAt.Equal(origin.Add(2*tick)) {
				t.Fatal("actual persisted version times differ from the controlled application writes")
			}
			for _, phase := range []struct {
				name         string
				app, durable time.Duration
				want         []string
			}{
				{"before", 24*time.Hour - 3*tick, 0, nil},
				{"exactly", tick, 0, nil},
				{"after", tick, 0, []string{first.ID, second.ID}},
				{"durable-only", 0, 48 * time.Hour, []string{first.ID, second.ID}},
			} {
				b24StepClocks(t, f, application, phase.app, phase.durable)
				b24ObserveClocks(t, f, application, phase.name)
				b24StaleRead(t, f, "independent-"+phase.name, phase.want)
				before := f.state(t)
				b24ReopenClocks(t, f, application)
				sameState(t, before, f.state(t))
				b24StaleRead(t, f, "independent-"+phase.name+"-readback", phase.want)
			}
			// A continuation keeps its original application cutoff even when
			// application time later makes the third row stale in a fresh query.
			params := map[string]any{"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1}}
			var emitted []string
			var cursor string
			b24ConservedRead(t, f, "independent-pinned-first", func() {
				emitted, cursor = staleIDs(t, f.call("view.run", "independent-pinned-first", params))
				if len(emitted) != 1 || cursor == "" {
					t.Fatal("two stale rows did not create a continuation")
				}
			})
			b24StepClocks(t, f, application, 7*24*time.Hour, 0)
			b24ReopenClocks(t, f, application)
			params["cursor"] = cursor
			b24ConservedRead(t, f, "independent-pinned-second", func() {
				remaining, next := staleIDs(t, f.call("view.run", "independent-pinned-second", params))
				if len(remaining) != 1 || next != "" || remaining[0] == emitted[0] || remaining[0] == newer.ID || emitted[0] == newer.ID {
					t.Fatalf("pinned membership changed: first=%v remaining=%v next=%q", emitted, remaining, next)
				}
			})
			b24StaleRead(t, f, "independent-fresh-later", []string{first.ID, second.ID, newer.ID})
		})
	}
}

func TestS1B24IndependentClockDeferReceiptAndReopenAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.ids = []string{"b24-clock-deferred"}
			application := testclock.New(t, f.clock.Now())
			b24BindApplicationClock(t, f, application)
			tick := time.Nanosecond
			if engine == "postgres" {
				tick = time.Microsecond
			}
			ref, _ := f.create(t, "independent-defer-target", "defer independently")
			b24ConservedRead(t, f, "independent-blank-reason", func() {
				refused := f.call("bead.defer", "independent-blank-reason", map[string]any{"id": ref, "reason": " "})
				if refused.Error == nil || refused.Error.Data.Code != "invalid_params" {
					t.Fatalf("blank reason response=%+v", refused)
				}
			})
			request := f.request("bead.defer", "independent-defer", map[string]any{"id": ref, "reason": "waiting independently"})
			original := f.dispatch(request)
			deferred := commit(t, original)
			if len(deferred.Results) != 2 {
				t.Fatal("defer with reason must return the patch and comment results")
			}
			for _, result := range deferred.Results {
				if result.Ref.ID != ref.ID || result.Revision != deferred.Revision {
					t.Fatal("deferral results lost the target or common revision")
				}
				document, err := pipeline.DecodeDocument(result.Record)
				if err != nil || document.Fields["status"] != "backlog" || len(document.Comments) != 1 || document.Comments[0].Body != "waiting independently" {
					t.Fatal("deferral did not return its final document and exactly one reason")
				}
			}
			receiptKey := "bead.defer/" + key("independent-defer")
			parked := f.state(t)
			if parked.Receipts[receiptKey] == nil {
				t.Fatal("deferral did not persist its original receipt")
			}
			for _, phase := range []struct {
				name         string
				app, durable time.Duration
				visible      bool
			}{
				{"application-only-after", driver.IdempotencyWindow + tick, 0, true},
				{"durable-before", 0, driver.IdempotencyWindow - tick, true},
				{"durable-exactly", 0, tick, false},
				{"durable-after-equal", 0, tick, false},
			} {
				b24StepClocks(t, f, application, phase.app, phase.durable)
				b24ObserveClocks(t, f, application, phase.name)
				for readback := 0; readback < 2; readback++ {
					if readback == 1 {
						before := f.state(t)
						b24ReopenClocks(t, f, application)
						sameState(t, before, f.state(t))
					}
					b24ConservedRead(t, f, fmt.Sprintf("%s-readback-%d", phase.name, readback), func() {
						current := f.state(t)
						if (current.Receipts[receiptKey] != nil) != phase.visible {
							t.Fatalf("%s receipt visibility followed the wrong clock", phase.name)
						}
						old, actual := parked, current
						old.Receipts, actual.Receipts = nil, nil
						sameState(t, old, actual)
						if phase.visible {
							replayed := f.dispatch(request)
							if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(original.Result, replayed.Result) {
								t.Fatalf("%s original deferral replay changed: %+v", phase.name, replayed)
							}
						}
						b24Ready(t, f, phase.name, "")
					})
				}
			}
			if !application.Now().Equal(f.clock.Now()) {
				t.Fatal("final independent clock inputs did not converge")
			}
			b24ConservedRead(t, f, "independent-timed-refusal", func() {
				refused := f.call("bead.defer", "independent-timed-refusal", map[string]any{"id": ref, "reason": "still waiting", "until": application.Now().Add(time.Hour)})
				if refused.Error == nil || refused.Error.Data.Code != "invalid_params" {
					t.Fatal("adopted untimed defer accepted a target time")
				}
			})
			commit(t, f.call("bead.reopen", "independent-explicit-reopen", map[string]any{"id": ref, "reason": "available now"}))
			b24ConservedRead(t, f, "independent-ready-reopened", func() { b24Ready(t, f, "independent-reopened", ref.ID) })

			// Now advance the independently bound inputs together. SQL and Go
			// must agree at each representable side of this new receipt's expiry.
			aligned := f.request("bead.defer", "independent-aligned-defer", map[string]any{"id": ref, "reason": "waiting with equal inputs"})
			alignedResponse := f.dispatch(aligned)
			commit(t, alignedResponse)
			alignedKey := "bead.defer/" + key("independent-aligned-defer")
			for _, phase := range []struct {
				name    string
				delta   time.Duration
				visible bool
			}{
				{"aligned-before", driver.IdempotencyWindow - tick, true},
				{"aligned-exactly", tick, false},
				{"aligned-after", tick, false},
			} {
				b24StepClocks(t, f, application, phase.delta, phase.delta)
				if !application.Now().Equal(f.clock.Now()) {
					t.Fatal("equal independent clock inputs diverged")
				}
				b24ObserveClocks(t, f, application, phase.name)
				b24ReopenClocks(t, f, application)
				b24ConservedRead(t, f, phase.name, func() {
					if (f.state(t).Receipts[alignedKey] != nil) != phase.visible {
						t.Fatalf("%s changed the strict receipt boundary", phase.name)
					}
					if phase.visible {
						replayed := f.dispatch(aligned)
						if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(alignedResponse.Result, replayed.Result) {
							t.Fatal("aligned receipt replay changed its original response")
						}
					}
					b24Ready(t, f, phase.name, "")
				})
			}
			commit(t, f.call("bead.reopen", "independent-aligned-reopen", map[string]any{"id": ref, "reason": "available after equal inputs"}))
			b24ConservedRead(t, f, "independent-aligned-ready", func() { b24Ready(t, f, "independent-aligned-reopened", ref.ID) })
		})
	}
}

func b24Ready(t *testing.T, f *fixture, label, expectedID string) {
	t.Helper()
	response := f.call("bead.ready", "independent-ready-"+label, map[string]any{"limit": 100})
	if response.Error != nil {
		t.Fatalf("ready: %+v", response.Error)
	}
	var page struct {
		Items []pipeline.Document `json:"items"`
	}
	if err := json.Unmarshal(response.Result, &page); err != nil {
		t.Fatal(err)
	}
	if expectedID == "" && len(page.Items) != 0 || expectedID != "" && (len(page.Items) != 1 || page.Items[0].ID != expectedID) {
		t.Fatalf("ready membership=%s want ID=%q", response.Result, expectedID)
	}
}
