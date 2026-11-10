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

func selectionRowTransportFixture() []selectionDBRow {
	const count = publicationBatchRows*2 + 3
	rows := make([]selectionDBRow, count)
	for i := range rows {
		id := fmt.Sprintf("row-transport-%04d\x00\xff", i)
		template, priority, kind := i%2 == 0, int64(i), "ancestor"
		rows[i] = selectionDBRow{
			RowKey: selectionHex(id), SourceKey: selectionHex("source-" + id),
			SourceRevision: "18446744073709551615", SourcePresent: i%2 == 0,
			SourceDigest: selectionHex("digest-" + id), ValueDigest: selectionHex("value-" + id),
			BeadKey: selectionHexPtr("bead-" + id), Ref: selectionHexPtr("ref-" + id),
			ID: selectionHexPtr(id), Type: selectionHexPtr("type-" + id),
			Project: selectionHexPtr("project-" + id), Parent: selectionHexPtr("parent-" + id),
			Status: selectionHexPtr("status-" + id), Title: selectionHexPtr("title-" + id),
			Leaf: selectionHexPtr("leaf-" + id), Template: &template, Priority: &priority,
			FactKind: &kind, FamilyTable: "ancestor_closure", SourceKind: "derived",
			SourceTable: "sort_key", Layout: 2, Annotation: []byte{0, byte(i), 255},
		}
		row := &rows[i]
		switch i % 4 {
		case 0:
			priority = math.MinInt64
		case 1:
			priority, kind = math.MaxInt64, ""
			row.BeadKey, row.Ref, row.ID, row.Type = selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr("")
			row.Project, row.Parent, row.Status, row.Title, row.Leaf = selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr(""), selectionHexPtr("")
			row.SourceRevision, row.SourceDigest, row.ValueDigest, row.SourceTable = "0", "", "", ""
			row.Annotation = []byte{}
		case 2:
			row.BeadKey, row.Ref, row.ID, row.Type = nil, nil, nil, nil
			row.Project, row.Parent, row.Status, row.Title, row.Leaf = nil, nil, nil, nil, nil
			row.Template, row.Priority, row.FactKind = nil, nil, nil
		case 3:
			row.Project, row.Title, row.Template, row.Priority = nil, nil, nil, nil
		}
	}
	return rows
}

// The expected relation is the exact predecessor scalar JSON/hex transport,
// including its separate binary annotation array. Compare every physical
// column across three batches, without using a logical decoder as the oracle.
func TestSelectionRowBinaryPublicationMatchesLegacy(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE row_transport_legacy (LIKE plug_postgres.selection_rows INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	rows := selectionRowTransportFixture()
	annotations := make([][]byte, len(rows))
	for i, row := range rows {
		annotations[i] = row.Annotation
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO row_transport_legacy(incarnation,start_revision,`+selectionColumns+`,annotation) SELECT $1,$2,`+selectionJSONColumns+`,($4::bytea[])[p.ordinality::int] FROM ROWS FROM (jsonb_to_recordset($3::jsonb) AS (`+selectionJSONLayout+`)) WITH ORDINALITY AS p`, "row-transport", "18446744073709551615", selectionJSON(rows), annotations); err != nil {
		t.Fatal(err)
	}
	if err = writeSelectionRows(ctx, tx, "row-transport", model.Natural64(math.MaxUint64), rows, nil); err != nil {
		t.Fatal(err)
	}
	var actual, differences int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_rows WHERE incarnation='row-transport'`).Scan(&actual); err != nil || actual != len(rows) {
		t.Fatalf("binary rows=%d want=%d: %v", actual, len(rows), err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM ((SELECT * FROM plug_postgres.selection_rows WHERE incarnation='row-transport' EXCEPT ALL SELECT * FROM row_transport_legacy) UNION ALL (SELECT * FROM row_transport_legacy EXCEPT ALL SELECT * FROM plug_postgres.selection_rows WHERE incarnation='row-transport')) differences`).Scan(&differences); err != nil || differences != 0 {
		t.Fatalf("complete binary/legacy physical row differences=%d: %v", differences, err)
	}
}

type selectionRowTransportTx struct {
	*sql.Tx
	successfulWrites int
	publishedRows    int64
}

func (tx *selectionRowTransportTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := tx.Tx.ExecContext(ctx, query, args...)
	if err == nil {
		tx.successfulWrites++
		count, countErr := result.RowsAffected()
		if countErr != nil {
			return nil, countErr
		}
		tx.publishedRows += count
	}
	return result, err
}

// Each late refusal follows one successful 256-row publication. Rollback must
// conserve the complete row/fact and mutation-bookkeeping images, with the same
// SQL refusal as the predecessor. Cancellation here is pre-canceled only.
func TestSelectionRowBinaryPublicationPreservesFailureRollback(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
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
	for _, failure := range []string{"duplicate_second_batch", "future_source_second_batch", "null_annotation_second_batch", "layout_overflow_second_batch", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			rows := selectionRowTransportFixture()[:publicationBatchRows+1]
			for i := range rows {
				rows[i].SourceRevision = "0"
			}
			switch failure {
			case "duplicate_second_batch":
				rows[publicationBatchRows].RowKey = rows[0].RowKey
			case "future_source_second_batch":
				rows[publicationBatchRows].SourceRevision = "2"
			case "null_annotation_second_batch":
				rows[publicationBatchRows].Annotation = nil
			case "layout_overflow_second_batch":
				rows[publicationBatchRows].Layout = 1 << 40
			}
			var states []string
			for _, binary := range []bool{false, true} {
				inner, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				tx := &selectionRowTransportTx{Tx: inner}
				callCtx, cancel := context.WithCancel(ctx)
				if failure == "cancelled" {
					cancel()
				}
				if binary {
					err = writeSelectionRows(callCtx, tx, "row-transport", model.Natural64(1), rows, nil)
				} else {
					err = selectionRowLegacyPublication(callCtx, tx, rows)
				}
				cancel()
				_ = tx.Rollback()
				if err == nil {
					t.Fatalf("binary=%t admitted %s", binary, failure)
				}
				if failure == "cancelled" {
					if !errors.Is(err, context.Canceled) || tx.successfulWrites != 0 {
						t.Fatalf("binary=%t cancellation=%v completed=%d", binary, err, tx.successfulWrites)
					}
				} else {
					if tx.successfulWrites != 1 || tx.publishedRows != publicationBatchRows {
						t.Fatalf("binary=%t failed before the complete first batch: writes=%d rows=%d: %v", binary, tx.successfulWrites, tx.publishedRows, err)
					}
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

func selectionRowLegacyPublication(ctx context.Context, tx *selectionRowTransportTx, rows []selectionDBRow) error {
	for start := 0; start < len(rows); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(rows))
		annotations := make([][]byte, end-start)
		for i, row := range rows[start:end] {
			annotations[i] = row.Annotation
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO plug_postgres.selection_rows(incarnation,start_revision,`+selectionColumns+`,annotation) SELECT $1,$2,`+selectionJSONColumns+`,($4::bytea[])[p.ordinality::int] FROM ROWS FROM (jsonb_to_recordset($3::jsonb) AS (`+selectionJSONLayout+`)) WITH ORDINALITY AS p`, "row-transport", "1", selectionJSON(rows[start:end]), annotations); err != nil {
			return err
		}
	}
	return nil
}
