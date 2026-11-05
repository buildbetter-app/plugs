package sqlite

import (
	"context"
	"errors"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"path/filepath"
	"testing"
)

func TestReadyOrderWriterFinalizesAfterGraphCertificateBump(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "graph-ready.db"))
	t.Cleanup(func() { _ = s.Close() })
	f := readyorder.NewCertificateFixture(t, s)
	graphHead := func() string {
		t.Helper()
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		states, err := r.Observe(ctx, []driver.LockKey{pipeline.GraphValidityLockKey()})
		closeErr := r.Close()
		if err != nil || closeErr != nil || len(states) != 1 || !states[0].Present || states[0].Revision.String() != meta.Head.String() {
			t.Fatalf("Writer did not advance its graph certificate with Head: head=%s states=%+v err=%v close=%v", meta.Head.String(), states, err, closeErr)
		}
		return meta.Head.String()
	}
	before := graphHead()
	f.Update(t)
	if after := graphHead(); after == before {
		t.Fatal("ordinary Writer update did not advance graph certificate")
	}
	result, err := f.Select(ctx)
	if err != nil || result.Total != 1 || len(result.Keys) != 1 || result.Keys[0].Ref.ID != f.Ref.ID {
		t.Fatalf("graph-certified Writer update lost Ready publication: %+v %v", result, err)
	}
}

func TestReadyOrderPhysicalRawMutation(t *testing.T) {
	for _, statement := range []string{"UPDATE records SET data=data", "UPDATE versions SET data=data", "UPDATE key_history SET data=data", "UPDATE selection_source_history SET source_digest=source_digest", "INSERT OR REPLACE INTO records SELECT * FROM records", "INSERT OR REPLACE INTO versions SELECT * FROM versions", "INSERT OR REPLACE INTO key_history SELECT * FROM key_history"} {
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
			// Repeated replacement statements must merge the same journal keys
			// without allowing their outer conflict policy to reject the write.
			if _, err = rolled.ExecContext(ctx, statement); err != nil {
				t.Fatal("repeated raw mutation rejected", err)
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

func TestReadyOrderPhysicalHiddenSource(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, context.Background(), filepath.Join(t.TempDir(), "proof.db"))
	f := readyorder.NewCertificateFixture(t, s)
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER ready_test_hidden_source AFTER UPDATE ON records BEGIN INSERT OR REPLACE INTO records(key,data) VALUES('raw-intruder',NEW.data); END;`); err != nil {
		t.Fatal(err)
	}
	f.Update(t)
	f.RequireUnavailable(t)
}

func TestReadyOrderPhysicalEmptyBootstrap(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := openStore(t, context.Background(), filepath.Join(t.TempDir(), "proof.db"))
		result, err := readyorder.PublishEmptyCertificate(t, s)
		if err != nil || result.Total != 0 {
			t.Fatalf("empty startup did not certify: %+v %v", result, err)
		}
	})
	t.Run("preexisting_source_image", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "legacy.db")
		value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		old := value.(*Store)
		m, err := old.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Reproduce an additive upgrade: physical source images predate installation
		// of Ready's fence. Coverage metadata alone is allowed; populated images are not.
		if _, err = old.db.ExecContext(ctx, `DROP TRIGGER ready_order_selection_source_history_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err = old.db.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,1,'unproved')`, m.Incarnation, revKey(0), "phantom", revKey(0), revKey(0)); err != nil {
			t.Fatal(err)
		}
		if _, err = old.db.ExecContext(ctx, `DELETE FROM ready_order_fence`); err != nil {
			t.Fatal(err)
		}
		if err = old.Close(); err != nil {
			t.Fatal(err)
		}
		value, err = (Driver{}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		s := value.(*Store)
		defer s.Close()
		var virgin bool
		if err = s.db.QueryRowContext(ctx, `SELECT ever_source=0 FROM ready_order_fence`).Scan(&virgin); err != nil || !virgin {
			t.Fatal("fixture must exercise fresh unproved lifetime", virgin, err)
		}
		result, err := readyorder.PublishEmptyCertificate(t, s)
		if !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("populated upgrade certified an empty tree: %+v %v", result, err)
		}
	})
}
