package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
)

type readyFenceImage struct {
	generation uint64
	dirty      bool
	everSource bool
	tuple      string
}

func readReadyFenceImage(t *testing.T, q querier) readyFenceImage {
	t.Helper()
	var image readyFenceImage
	var generation string
	if err := q.QueryRowContext(context.Background(), `SELECT generation::text,dirty,ever_source,ctid::text FROM plug_postgres.ready_order_fence WHERE singleton`).Scan(&generation, &image.dirty, &image.everSource, &image.tuple); err != nil {
		t.Fatal(err)
	}
	var err error
	if image.generation, err = parseCounter(generation); err != nil {
		t.Fatal(err)
	}
	return image
}

func TestReadyOrderDirtyFenceAvoidsRepeatedUpdates(t *testing.T) {
	ctx := context.Background()
	t.Run("virgin_source_tracking_and_meta_lock", func(t *testing.T) {
		s := factory(t).(*Store)
		before := readReadyFenceImage(t, s.db)
		if !before.dirty || before.everSource || before.generation != 0 {
			t.Fatalf("not a virgin dirty fence: %+v", before)
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, `UPDATE plug_postgres.selection_coverage SET format=format WHERE false`); err != nil {
			t.Fatal(err)
		}
		if got := readReadyFenceImage(t, tx); got != before {
			t.Fatalf("already dirty non-source statement rewrote the fence: %+v -> %+v", before, got)
		}
		// A no-op fence transition must still hold the same meta-first lock.
		blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err = s.db.ExecContext(blocked, `UPDATE plug_postgres.selection_coverage SET format=format WHERE false`)
		cancel()
		if err == nil || !errors.Is(blocked.Err(), context.DeadlineExceeded) {
			t.Fatalf("dirty statement bypassed the meta lock: %v", err)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE plug_postgres.records SET body=body WHERE false`); err != nil {
			t.Fatal(err)
		}
		source := readReadyFenceImage(t, tx)
		if !source.dirty || !source.everSource || source.generation != before.generation || source.tuple == before.tuple {
			t.Fatalf("first source statement lost conservative lifetime tracking: %+v -> %+v", before, source)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE plug_postgres.versions SET value=value WHERE false`); err != nil {
			t.Fatal(err)
		}
		if got := readReadyFenceImage(t, tx); got != source {
			t.Fatalf("repeated source statement rewrote the dirty fence: %+v -> %+v", source, got)
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got := readReadyFenceImage(t, s.db); got != before {
			t.Fatalf("rollback retained a source lifetime or generation change: %+v -> %+v", before, got)
		}
	})
	t.Run("certified_raw_journal_and_rollback", func(t *testing.T) {
		s := factory(t).(*Store)
		fixture := readyorder.NewCertificateFixture(t, s)
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before := readReadyFenceImage(t, s.db)
		if before.dirty || !before.everSource {
			t.Fatalf("producer did not certify its source: %+v", before)
		}
		mutate := func(commit bool) {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			result, err := tx.ExecContext(ctx, `UPDATE plug_postgres.records SET body=body`)
			if err != nil {
				t.Fatal(err)
			}
			if count, err := result.RowsAffected(); err != nil || count == 0 {
				t.Fatal("no actual source mutation", count, err)
			}
			first := readReadyFenceImage(t, tx)
			if !first.dirty || !first.everSource || first.generation != before.generation+1 || first.tuple == before.tuple {
				t.Fatalf("first raw statement did not invalidate exactly once: %+v -> %+v", before, first)
			}
			for _, statement := range []string{
				`UPDATE plug_postgres.records SET body=body`,
				`UPDATE plug_postgres.versions SET value=value`,
				`UPDATE plug_postgres.states SET value=value`,
				`UPDATE plug_postgres.selection_sources SET digest=digest`,
			} {
				if _, err = tx.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
				if got := readReadyFenceImage(t, tx); got != first {
					t.Fatalf("repeated raw statement rewrote the fence: %+v -> %+v", first, got)
				}
			}
			// Both source and state journals must retain their complete real keys
			// even when none of the repeated statements rewrites the fence tuple.
			var exact bool
			if err = tx.QueryRowContext(ctx, `WITH expected AS (
			 SELECT 'source'::text AS domain,key FROM plug_postgres.records
			 UNION SELECT 'source',key FROM plug_postgres.versions
			 UNION SELECT 'state',key FROM plug_postgres.states
			), missing AS (SELECT * FROM expected EXCEPT SELECT * FROM plug_postgres.ready_order_changes),
			 extra AS (SELECT * FROM plug_postgres.ready_order_changes EXCEPT SELECT * FROM expected)
			 SELECT NOT EXISTS(SELECT 1 FROM missing) AND NOT EXISTS(SELECT 1 FROM extra)`).Scan(&exact); err != nil || !exact {
				t.Fatal("raw journal lost or invented keys", exact, err)
			}
			if commit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		mutate(false)
		if got := readReadyFenceImage(t, s.db); got != before {
			t.Fatalf("raw rollback changed the certified fence: %+v -> %+v", before, got)
		}
		if _, err = fixture.Select(ctx); err != nil {
			t.Fatal("raw rollback damaged the certificate", err)
		}
		mutate(true)
		after, err := s.Meta(ctx)
		if err != nil || after.Head != meta.Head {
			t.Fatal("raw mutation changed logical Head", after, err)
		}
		fixture.RequireUnavailable(t)
		fixture.Update(t)
		fixture.RequireUnavailable(t)
	})
}
