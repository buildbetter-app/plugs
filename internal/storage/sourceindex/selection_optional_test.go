package sourceindex

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	_ "modernc.org/sqlite"
)

// Feed the actual SQL lifecycle result through the production public caller.
// The SQL transaction above that boundary controls publication; this store
// supplies the normal Meta contract without recreating or changing the error.
type selectionBackfillCallerStore struct {
	driver.Store
	result error
}

func (s selectionBackfillCallerStore) BuildGraphSourceIndexes(context.Context, driver.ActiveLease, driver.GraphSourceDecoder, ...driver.GraphSourceProjection) error {
	return s.result
}

func requireSelectionBackfillCallerResult(t *testing.T, result error, optional bool) {
	t.Helper()
	store := memory.New()
	defer store.Close()
	err := pipeline.BuildSourceIndexes(context.Background(), selectionBackfillCallerStore{Store: store, result: result}, nil)
	if optional {
		if err != nil {
			t.Fatalf("public caller lost safe completed selection fallback: %v", err)
		}
	} else if err != result {
		t.Fatalf("public caller suppressed or changed the actual fatal SQL result: want=%v got=%v", result, err)
	}
}

func optionalBackfillDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	statements := []string{
		`CREATE TABLE records(key TEXT PRIMARY KEY,data BLOB)`,
		`CREATE TABLE objects(key TEXT PRIMARY KEY,kind TEXT)`,
		`CREATE TABLE optional_staging(step TEXT)`,
		`CREATE TABLE optional_markers(family TEXT PRIMARY KEY)`,
	}
	for _, table := range []string{"hierarchy_index_coverage", "graph_link_index_coverage"} {
		statements = append(statements, `CREATE TABLE `+table+`(incarnation TEXT,branch TEXT,format INTEGER,built_from TEXT,certified_through TEXT,state TEXT,PRIMARY KEY(incarnation,branch))`)
	}
	statements = append(statements,
		`CREATE TABLE hierarchy_source_current(incarnation TEXT,branch TEXT,child_key TEXT,parent_key TEXT,ref BLOB,kind TEXT,revision TEXT,body BLOB)`,
		`CREATE TABLE hierarchy_source_history(incarnation TEXT,branch TEXT,child_key TEXT,parent_key TEXT,start_revision TEXT,end_revision TEXT,ref BLOB,kind TEXT,revision TEXT,body BLOB)`,
		`CREATE TABLE graph_link_source_current(incarnation TEXT,branch TEXT,source_key TEXT,ref BLOB,kind TEXT,revision TEXT,body BLOB)`,
		`CREATE TABLE graph_link_source_history(incarnation TEXT,branch TEXT,source_key TEXT,start_revision TEXT,end_revision TEXT,ref BLOB,kind TEXT,revision TEXT,body BLOB)`,
		`CREATE TABLE graph_link_current(incarnation TEXT,branch TEXT,source_key TEXT,link_kind BLOB,target_key TEXT)`,
		`CREATE TABLE graph_link_history(incarnation TEXT,branch TEXT,source_key TEXT,start_revision TEXT,link_kind BLOB,target_key TEXT)`)
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	record := driver.Record{Ref: model.RefByID("optional-source", model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: []byte(`{"opaque":"source"}`)}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO records VALUES(?,?)`, key(record.Ref), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO objects VALUES(?,'bead')`, key(record.Ref)); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestSelectionBackfillRefusalKeepsGraphActivation(t *testing.T) {
	for _, phase := range []string{"clear", "page", "activate"} {
		for _, refusal := range []error{driver.ErrSelectionUnavailable, driver.ErrUnsupported} {
			t.Run(phase+"/"+refusal.Error(), func(t *testing.T) {
				db := optionalBackfillDB(t)
				ctx := context.Background()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				calls := map[string]int{}
				step := func(name string, q SQL) error {
					calls[name]++
					if _, err := q.ExecContext(ctx, `INSERT INTO optional_staging VALUES(?)`, name); err != nil {
						return err
					}
					if name == "activate" {
						for _, family := range []string{"sort", "label", "link", "ancestor"} {
							if _, err := q.ExecContext(ctx, `INSERT INTO optional_markers VALUES(?)`, family); err != nil {
								return err
							}
						}
					}
					if name == phase {
						return fmt.Errorf("optional %s: %w", name, refusal)
					}
					return nil
				}
				hooks := BackfillSelection{
					Clear: func(ctx context.Context, publish BackfillPublication) error {
						return publish(ctx, func(q SQL) error { return step("clear", q) })
					},
					Page: func(_ context.Context, q SQL, records []driver.Record, _ []driver.GraphSourceMetadata) error {
						if len(records) != 1 {
							t.Fatal("real source page was not decoded")
						}
						return step("page", q)
					},
					Activate: func(_ context.Context, q SQL) error { return step("activate", q) },
				}
				err = SQLite.Backfill(ctx, tx, driver.Meta{Incarnation: "optional", Head: model.Natural64(1)}, func(driver.Record) (driver.GraphSourceMetadata, error) {
					return driver.GraphSourceMetadata{}, nil
				}, hooks)
				if !IsSelectionBackfillRefused(err) || !errors.Is(err, refusal) {
					t.Fatalf("graph-completed refusal lost: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				requireSelectionBackfillCallerResult(t, err, true)
				coverage, err := SQLite.ReadCoverage(ctx, db, "optional")
				if err != nil || !coverage.Parent.Ready || !coverage.Links.Ready || coverage.Parent.Require(1) != nil || coverage.Links.Require(1) != nil {
					t.Fatalf("graph activation rolled back: %+v/%v", coverage, err)
				}
				for _, table := range []string{"hierarchy_source_current", "graph_link_source_current"} {
					var count int
					if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 1 {
						t.Fatalf("graph source missing: %s=%d/%v", table, count, err)
					}
				}
				var markers, refusedEffects int
				if err := db.QueryRow(`SELECT count(*) FROM optional_markers`).Scan(&markers); err != nil || markers != 0 {
					t.Fatalf("partial selection activation escaped: %d/%v", markers, err)
				}
				if err := db.QueryRow(`SELECT count(*) FROM optional_staging WHERE step=?`, phase).Scan(&refusedEffects); err != nil || refusedEffects != 0 {
					t.Fatalf("refused step escaped its savepoint: %d/%v", refusedEffects, err)
				}
				if calls[phase] != 1 || phase == "clear" && (calls["page"] != 0 || calls["activate"] != 0) || phase == "page" && calls["activate"] != 0 {
					t.Fatalf("refused lifecycle continued: %v", calls)
				}
			})
		}
	}
}

func TestSelectionBackfillRefusalDoesNotHideFatalOrGraphErrors(t *testing.T) {
	for _, fatal := range []error{context.Canceled, driver.ErrFenced{}, errors.New("SQL transport failed")} {
		joined := errors.Join(driver.ErrSelectionUnavailable, fatal)
		if IsSelectionBackfillUnavailable(joined) || IsSelectionBackfillRefused(joined) {
			t.Fatalf("mixed fatal classified optional: %v", joined)
		}
	}
	if !IsSelectionBackfillUnavailable(errors.Join(driver.ErrSelectionUnavailable, fmt.Errorf("wrapped: %w", driver.ErrUnsupported))) {
		t.Fatal("joined optional refusals not recognized")
	}
	db := optionalBackfillDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = SQLite.Backfill(context.Background(), tx, driver.Meta{Incarnation: "optional", Head: model.Natural64(1)}, func(driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{}, driver.ErrUnsupported
	})
	if !errors.Is(err, driver.ErrUnsupported) || IsSelectionBackfillRefused(err) {
		t.Fatalf("graph error gained a completion tag: %v", err)
	}
}

type failingSelectionSavepointSQL struct {
	SQL
	phase, failPhase int
	statement        string
	failure          error
	fired            bool
}

func (q *failingSelectionSavepointSQL) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	if statement == "SAVEPOINT plug_optional_selection_backfill" {
		q.phase++
	}
	if !q.fired && q.phase == q.failPhase && statement == q.statement {
		q.fired = true
		return nil, q.failure
	}
	return q.SQL.ExecContext(ctx, statement, args...)
}

func TestSelectionBackfillSQLFailuresStayFatal(t *testing.T) {
	identities := []struct {
		name string
		err  error
	}{
		{"unavailable", driver.ErrSelectionUnavailable},
		{"unsupported", driver.ErrUnsupported},
		{"wrapped_unsupported", fmt.Errorf("SQL driver: %w", driver.ErrUnsupported)},
		{"sql", errors.New("SQL cleanup failed")},
	}
	operations := []struct {
		name, statement string
		refuse          bool
	}{
		{"create", "SAVEPOINT plug_optional_selection_backfill", true},
		{"rollback", "ROLLBACK TO SAVEPOINT plug_optional_selection_backfill", true},
		{"release_after_refusal", "RELEASE SAVEPOINT plug_optional_selection_backfill", true},
		{"release_after_success", "RELEASE SAVEPOINT plug_optional_selection_backfill", false},
	}
	for phaseIndex, phase := range []string{"clear", "page", "activate"} {
		for _, operation := range operations {
			for _, identity := range identities {
				t.Run(phase+"/"+operation.name+"/"+identity.name, func(t *testing.T) {
					db := optionalBackfillDB(t)
					ctx := context.Background()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					q := &failingSelectionSavepointSQL{SQL: tx, failPhase: phaseIndex + 1, statement: operation.statement, failure: identity.err}
					step := func(name string, q SQL) error {
						if _, err := q.ExecContext(ctx, `INSERT INTO optional_staging VALUES(?)`, name); err != nil {
							return err
						}
						if name == phase && operation.refuse {
							return fmt.Errorf("optional %s: %w", name, driver.ErrSelectionUnavailable)
						}
						return nil
					}
					hooks := BackfillSelection{
						Clear: func(ctx context.Context, publish BackfillPublication) error {
							return publish(ctx, func(q SQL) error { return step("clear", q) })
						},
						Page: func(_ context.Context, q SQL, _ []driver.Record, _ []driver.GraphSourceMetadata) error {
							return step("page", q)
						},
						Activate: func(_ context.Context, q SQL) error { return step("activate", q) },
					}
					err = SQLite.Backfill(ctx, q, driver.Meta{Incarnation: "optional", Head: model.Natural64(1)}, func(driver.Record) (driver.GraphSourceMetadata, error) {
						return driver.GraphSourceMetadata{}, nil
					}, hooks)
					if !q.fired || !errors.Is(err, identity.err) || IsSelectionBackfillUnavailable(err) || IsSelectionBackfillRefused(err) {
						t.Fatalf("savepoint failure became optional: fired=%v err=%v", q.fired, err)
					}
					requireSelectionBackfillCallerResult(t, err, false)
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					coverage, err := SQLite.ReadCoverage(ctx, db, "optional")
					if err != nil || coverage.Parent.Ready || coverage.Links.Ready {
						t.Fatalf("fatal lifecycle published graph coverage: %+v/%v", coverage, err)
					}
					for _, table := range []string{"optional_staging", "hierarchy_source_current", "graph_link_source_current"} {
						var count int
						if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
							t.Fatalf("fatal lifecycle escaped rollback: %s=%d/%v", table, count, err)
						}
					}
				})
			}
		}
	}
}
