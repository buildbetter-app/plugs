package postgres

import (
	"context"
	"errors"
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

func TestReadyOrderPhysicalHiddenSource(t *testing.T) {
	ctx := context.Background()
	s := factory(t).(*Store)
	f := readyorder.NewCertificateFixture(t, s)
	if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.ready_test_hidden_source() RETURNS trigger LANGUAGE plpgsql AS $proof$BEGIN INSERT INTO plug_postgres.records(key,ref,kind,revision,body,deleted,version_count) VALUES(convert_to('raw-intruder','UTF8'),NEW.ref,NEW.kind,NEW.revision,NEW.body,NEW.deleted,NEW.version_count); RETURN NULL; END;$proof$; CREATE TRIGGER ready_test_hidden_source AFTER UPDATE ON plug_postgres.records FOR EACH ROW EXECUTE FUNCTION plug_postgres.ready_test_hidden_source();`); err != nil {
		t.Fatal(err)
	}
	f.Update(t)
	f.RequireUnavailable(t)
}

func TestReadyOrderPhysicalEmptyBootstrap(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := factory(t).(*Store)
		result, err := readyorder.PublishEmptyCertificate(t, s)
		if err != nil || result.Total != 0 {
			t.Fatalf("empty startup did not certify: %+v %v", result, err)
		}
	})
	t.Run("preexisting_source_image", func(t *testing.T) {
		ctx := context.Background()
		dsn := freshDSN(t)
		value, err := (Driver{}).Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatal(err)
		}
		old := value.(*Store)
		m, err := old.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = old.db.ExecContext(ctx, `DROP TRIGGER a_ready_order_mutated ON plug_postgres.selection_sources;DROP TRIGGER ready_order_nonempty_source ON plug_postgres.selection_sources`); err != nil {
			t.Fatal(err)
		}
		if _, err = old.db.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) VALUES($1,$2,0,0,true,$3,'bead')`, m.Incarnation, []byte("phantom"), []byte("unproved")); err != nil {
			t.Fatal(err)
		}
		if _, err = old.db.ExecContext(ctx, `DELETE FROM plug_postgres.ready_order_fence`); err != nil {
			t.Fatal(err)
		}
		if err = old.Close(); err != nil {
			t.Fatal(err)
		}
		value, err = (Driver{}).Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatal(err)
		}
		s := value.(*Store)
		defer s.Close()
		var virgin bool
		if err = s.db.QueryRowContext(ctx, `SELECT NOT ever_source FROM plug_postgres.ready_order_fence`).Scan(&virgin); err != nil || !virgin {
			t.Fatal("fixture must exercise fresh unproved lifetime", virgin, err)
		}
		result, err := readyorder.PublishEmptyCertificate(t, s)
		if !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("populated upgrade certified an empty tree: %+v %v", result, err)
		}
	})
}
