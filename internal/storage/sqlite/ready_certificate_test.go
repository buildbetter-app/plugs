package sqlite

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"path/filepath"
	"testing"
)

func TestReadyOrderPhysicalRawMutation(t *testing.T) {
	for _, statement := range []string{"UPDATE records SET data=data", "UPDATE versions SET data=data", "UPDATE key_history SET data=data", "UPDATE selection_source_history SET source_digest=source_digest"} {
		t.Run(statement, func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t, context.Background(), filepath.Join(t.TempDir(), "proof.db"))
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
	readyorder.RunCertificateOverlay(t, func(t *testing.T) driver.Store {
		return openStore(t, context.Background(), filepath.Join(t.TempDir(), "proof.db"))
	})
}
