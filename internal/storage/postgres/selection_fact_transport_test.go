package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func selectionFactTransportFixture() ([]selectionDBRow, []selectionDBFact) {
	const count = publicationBatchRows + 3
	rows := make([]selectionDBRow, count)
	facts := make([]selectionDBFact, count)
	kind := "ancestor"
	for i := range rows {
		id := fmt.Sprintf("fact-transport-%04d\x00\xff", i)
		rows[i] = selectionDBRow{RowKey: selectionHex(id), SourceKey: selectionHex("owner-" + id),
			SourceRevision: "0", SourcePresent: true, FamilyTable: "ancestor_closure",
			SourceKind: "record", FactKind: &kind, Layout: 2, Annotation: []byte{0, byte(i), 255}}
		facts[i] = selectionDBFact{RowKey: selectionHex(id), Ordinal: i % 7,
			Ref: selectionHexPtr("ref-" + id), Target: selectionHexPtr("target-" + id),
			Subject: selectionHex("subject-" + id), Scope: selectionHex("scope-" + id),
			ScopeKind: selectionHex("project"), ProjectName: selectionHexPtr("project-" + id),
			Role: selectionHex("role-" + id), Name: selectionHex("name-" + id),
			Value: selectionHex("value-" + id), Active: i%2 == 0,
			Depth: int64(i), LinkKind: selectionHex("link-" + id)}
		if i%3 == 0 {
			facts[i].Ref, facts[i].Target, facts[i].ProjectName = nil, nil, nil
		} else if i%3 == 1 {
			facts[i].Ref, facts[i].Target, facts[i].ProjectName = selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr("")
			facts[i].Subject, facts[i].Scope, facts[i].ScopeKind = "", "", ""
			facts[i].Role, facts[i].Name, facts[i].Value, facts[i].LinkKind = "", "", "", ""
		}
	}
	facts[0].Depth, facts[1].Depth = math.MinInt64, math.MaxInt64
	return rows, facts
}

// Compare every physical column, including optional NULL versus empty bytes,
// opaque strings, ordinals and signed bigint bounds, over a second batch. The
// expected side uses the literal prior JSON/hex SQL, without a logical decoder.
func TestSelectionFactBinaryPublicationMatchesLegacy(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE fact_transport_legacy (LIKE plug_postgres.selection_facts INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	rows, facts := selectionFactTransportFixture()
	if _, err = tx.ExecContext(ctx, `INSERT INTO fact_transport_legacy(incarnation,start_revision,`+selectionFactColumns+`) SELECT $1,$2,`+selectionFactJSONColumns+` FROM jsonb_to_recordset($3::jsonb) AS f(`+selectionFactJSONLayout+`)`, "fact-transport", "18446744073709551615", selectionJSON(facts)); err != nil {
		t.Fatal(err)
	}
	if err = writeSelectionRows(ctx, tx, "fact-transport", model.Natural64(math.MaxUint64), rows, facts); err != nil {
		t.Fatal(err)
	}
	var actual, differences int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_facts WHERE incarnation='fact-transport'`).Scan(&actual); err != nil || actual != len(facts) {
		t.Fatalf("binary facts=%d want=%d: %v", actual, len(facts), err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM ((SELECT * FROM plug_postgres.selection_facts WHERE incarnation='fact-transport' EXCEPT ALL SELECT * FROM fact_transport_legacy) UNION ALL (SELECT * FROM fact_transport_legacy EXCEPT ALL SELECT * FROM plug_postgres.selection_facts WHERE incarnation='fact-transport')) differences`).Scan(&differences); err != nil || differences != 0 {
		t.Fatalf("complete binary/legacy physical fact differences=%d: %v", differences, err)
	}
}

// A later failing statement must keep the prior scalar/constraint refusal and
// roll back earlier successful batches, including mutation-fence bookkeeping.
func TestSelectionFactBinaryPublicationPreservesFailureRollback(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	rows, original := selectionFactTransportFixture()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeSelectionRows(ctx, tx, "fact-transport", model.Natural64(1), rows, nil); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot := func() []string {
		t.Helper()
		var image []string
		for _, table := range []string{"selection_rows", "selection_facts", "selection_mutation_fence", "selection_mutation_keys"} {
			var value string
			query := `SELECT coalesce(jsonb_agg(x ORDER BY x::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) AS x FROM plug_postgres.` + table + ` t) captured`
			if err := s.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
				t.Fatal(err)
			}
			image = append(image, value)
		}
		return image
	}
	before := snapshot()
	for _, failure := range []string{"duplicate_second_batch", "missing_parent_second_batch", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			facts := append([]selectionDBFact(nil), original[:publicationBatchRows+1]...)
			if failure == "duplicate_second_batch" {
				facts[publicationBatchRows] = facts[0]
			} else if failure == "missing_parent_second_batch" {
				facts[publicationBatchRows].RowKey = selectionHex("missing-parent")
			}
			var states []string
			for _, binary := range []bool{false, true} {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				callCtx, cancel := context.WithCancel(ctx)
				if failure == "cancelled" {
					cancel()
				}
				if binary {
					err = writeSelectionRows(callCtx, tx, "fact-transport", model.Natural64(1), nil, facts)
				} else {
					err = selectionFactLegacyPublication(callCtx, tx, facts)
				}
				cancel()
				_ = tx.Rollback()
				if err == nil {
					t.Fatalf("binary=%t admitted %s", binary, failure)
				}
				if failure == "cancelled" {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("binary=%t cancellation=%v", binary, err)
					}
				} else {
					var state interface{ SQLState() string }
					if !errors.As(err, &state) {
						t.Fatalf("binary=%t lost SQL refusal: %v", binary, err)
					}
					states = append(states, state.SQLState())
				}
				if after := snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatalf("binary=%t failed batch changed physical rows or mutation bookkeeping", binary)
				}
			}
			if len(states) == 2 && states[0] != states[1] {
				t.Fatalf("legacy/binary SQL refusal differs: %v", states)
			}
		})
	}
}

func selectionFactLegacyPublication(ctx context.Context, tx *sql.Tx, facts []selectionDBFact) error {
	for start := 0; start < len(facts); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(facts))
		if _, err := tx.ExecContext(ctx, `INSERT INTO plug_postgres.selection_facts(incarnation,start_revision,`+selectionFactColumns+`) SELECT $1,$2,`+selectionFactJSONColumns+` FROM jsonb_to_recordset($3::jsonb) AS f(`+selectionFactJSONLayout+`)`, "fact-transport", "1", selectionJSON(facts[start:end])); err != nil {
			return err
		}
	}
	return nil
}
