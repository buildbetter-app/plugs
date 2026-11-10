package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func selectionHeaderTransportFixture() []selectionSourceHeader {
	headers := make([]selectionSourceHeader, publicationBatchRows*2+3)
	for i := range headers {
		headers[i] = selectionSourceHeader{
			Key:      selectionHex(fmt.Sprintf("header-%04d\x00\xff", i)),
			Revision: "18446744073709551615", Present: i%2 == 0,
			Digest: selectionHex(fmt.Sprintf("digest-%04d\x00\xff", i)), Kind: "bead",
		}
		switch i % 3 {
		case 0:
			headers[i].Revision, headers[i].Digest, headers[i].Kind = "0", "", ""
		case 1:
			headers[i].Kind = "typed-雪\\\"header"
		}
	}
	headers[0].Key = ""
	return headers
}

// Compare every physical column with the predecessor JSON/hex statement. The
// actual helper must publish all 515 rows in the original three statements.
func TestSelectionHeaderBinaryPublicationMatchesLegacy(t *testing.T) {
	for _, kindCase := range []string{"ordinary_kind", "invalid_utf8_kind"} {
		t.Run(kindCase, func(t *testing.T) {
			ctx := context.Background()
			s := openTest(t, freshDSN(t))
			inner, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Rollback()
			if _, err = inner.ExecContext(ctx, `CREATE TEMP TABLE header_transport_legacy (LIKE plug_postgres.selection_sources INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
				t.Fatal(err)
			}
			headers := selectionHeaderTransportFixture()
			if kindCase == "invalid_utf8_kind" {
				headers[publicationBatchRows+2].Kind = "kind-\xff\xfe-\xe2\x82"
			}
			raw, err := json.Marshal(headers)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = inner.ExecContext(ctx, `INSERT INTO header_transport_legacy(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,decode(key,'hex'),$2,revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset($3::jsonb) AS s(key text,revision text,present boolean,digest text,kind text)`, "header-transport", "18446744073709551615", string(raw)); err != nil {
				t.Fatal(err)
			}
			tx := &selectionRowTransportTx{Tx: inner}
			if err = writeSelectionSourceHeaders(ctx, tx, "header-transport", model.Natural64(math.MaxUint64), headers); err != nil {
				t.Fatal(err)
			}
			if tx.successfulWrites != 3 || tx.publishedRows != int64(len(headers)) {
				t.Fatalf("header publication writes=%d rows=%d; want 3/%d", tx.successfulWrites, tx.publishedRows, len(headers))
			}
			var differences int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM ((SELECT * FROM plug_postgres.selection_sources WHERE incarnation='header-transport' EXCEPT ALL SELECT * FROM header_transport_legacy) UNION ALL (SELECT * FROM header_transport_legacy EXCEPT ALL SELECT * FROM plug_postgres.selection_sources WHERE incarnation='header-transport')) differences`).Scan(&differences); err != nil || differences != 0 {
				t.Fatalf("complete binary/legacy header differences=%d: %v", differences, err)
			}
		})
	}
}

// Refusals occur after one successful 256-header statement. Compare SQLSTATE
// with the exact old loop and conserve source rows and mutation bookkeeping
// after rollback. The cancellation case covers only a pre-canceled context.
func TestSelectionHeaderBinaryPublicationPreservesFailureRollback(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	snapshot := func() []string {
		t.Helper()
		var image []string
		for _, table := range []string{"selection_sources", "selection_mutation_fence", "selection_mutation_keys"} {
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
	for _, failure := range []string{"duplicate_second_batch", "future_revision_second_batch", "invalid_revision_second_batch", "nul_kind_second_batch", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			headers := selectionHeaderTransportFixture()[:publicationBatchRows+1]
			for i := range headers {
				headers[i].Revision = "0"
			}
			switch failure {
			case "duplicate_second_batch":
				headers[publicationBatchRows].Key = headers[0].Key
			case "future_revision_second_batch":
				headers[publicationBatchRows].Revision = "2"
			case "invalid_revision_second_batch":
				headers[publicationBatchRows].Revision = "not-a-revision"
			case "nul_kind_second_batch":
				headers[publicationBatchRows].Kind = "kind\x00refusal"
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
					err = writeSelectionSourceHeaders(callCtx, tx, "header-transport", model.Natural64(1), headers)
				} else {
					err = selectionHeaderLegacyPublication(callCtx, tx, headers)
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
						t.Fatalf("binary=%t failed before first complete batch: writes=%d rows=%d: %v", binary, tx.successfulWrites, tx.publishedRows, err)
					}
					var state interface{ SQLState() string }
					if !errors.As(err, &state) {
						t.Fatalf("binary=%t lost SQL refusal: %v", binary, err)
					}
					states = append(states, state.SQLState())
				}
				if after := snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatalf("binary=%t failed batch changed source rows or mutation bookkeeping", binary)
				}
			}
			if len(states) == 2 && states[0] != states[1] {
				t.Fatalf("legacy/binary SQL refusal differs: %v", states)
			}
		})
	}
}

func selectionHeaderLegacyPublication(ctx context.Context, tx *selectionRowTransportTx, headers []selectionSourceHeader) error {
	for start := 0; start < len(headers); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(headers))
		raw, _ := json.Marshal(headers[start:end])
		if _, err := tx.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,decode(key,'hex'),$2,revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset($3::jsonb) AS s(key text,revision text,present boolean,digest text,kind text)`, "header-transport", "1", string(raw)); err != nil {
			return err
		}
	}
	return nil
}
