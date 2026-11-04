package postgres

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"testing"
)

func TestReadyOrderPhysicalRawMutation(t *testing.T) {
	for _, statement := range []string{"UPDATE plug_postgres.records SET body=body", "UPDATE plug_postgres.versions SET value=value", "UPDATE plug_postgres.states SET value=value", "UPDATE plug_postgres.selection_sources SET digest=digest"} {
		t.Run(statement, func(t *testing.T) {
			ctx := context.Background()
			s := factory(t).(*Store)
			f := readyorder.NewCertificateFixture(t, s)
			// The same-byte write is still a raw mutation, at the SAME logical Head.
			before, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rolled, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := rolled.ExecContext(ctx, statement)
			if err != nil {
				t.Fatal(err)
			}
			n, err := result.RowsAffected()
			if err != nil || n == 0 {
				t.Fatal("no actual physical mutation", n, err)
			}
			if err = rolled.Rollback(); err != nil {
				t.Fatal(err)
			}
			if _, err = f.Select(ctx); err != nil {
				t.Fatal("rollback damaged certificate", err)
			}
			if _, err = s.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			after, err := s.Meta(ctx)
			if err != nil || after.Head != before.Head {
				t.Fatal("raw mutation changed Head", after, err)
			}
			f.RequireUnavailable(t)
			// A subsequent genuine incremental write must remain accepted but may not
			// recreate a proof across that untracked physical gap.
			f.Update(t)
			f.RequireUnavailable(t)
		})
	}
}

func TestReadyOrderPhysicalOverlay(t *testing.T) {
	readyorder.RunCertificateOverlay(t, func(t *testing.T) driver.Store { return factory(t).(*Store) })
}
