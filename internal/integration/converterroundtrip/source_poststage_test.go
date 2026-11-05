package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var errSourcePoststage = errors.New("injected source publication failure")

type sourcePoststageStore struct {
	driver.Store
	driver.WorkflowCheckpointStore
	mode            string
	staged, reached int
}

// Preserve the selected store's real persistence capability through the fault
// wrapper; the memory store truthfully remains process-scoped.
func (s *sourcePoststageStore) Persistent() bool {
	marker, ok := s.Store.(driver.PersistentCheckpointStore)
	return ok && marker.Persistent()
}

func (s *sourcePoststageStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	cp, ok := tx.(driver.CheckpointWriteTx)
	guard, gok := tx.(driver.CheckpointGuardTx)
	graph, rok := tx.(driver.GraphSourceWriter)
	if !ok || !gok || !rok {
		_ = tx.Close()
		return nil, driver.ErrUnsupported
	}
	return &sourcePoststageTx{WriteTx: tx, owner: s, checkpoint: cp, guard: guard, graph: graph}, nil
}

type sourcePoststageTx struct {
	driver.WriteTx
	owner      *sourcePoststageStore
	checkpoint driver.CheckpointWriteTx
	guard      driver.CheckpointGuardTx
	graph      driver.GraphSourceWriter
	staged     bool
}

func (t *sourcePoststageTx) Lock(ctx context.Context, req []driver.LockRequest) ([]driver.LockState, error) {
	state, err := t.WriteTx.Lock(ctx, req)
	if err == nil && t.owner.mode == "prime" {
		t.owner.reached++
		return nil, errSourcePoststage
	}
	return state, err
}
func (t *sourcePoststageTx) StageCheckpoint(ctx context.Context, key string, expected uint64, payload []byte) error {
	if err := t.checkpoint.StageCheckpoint(ctx, key, expected, payload); err != nil {
		return err
	}
	t.staged = true
	t.owner.staged++
	return nil
}
func (t *sourcePoststageTx) ExpectCheckpoint(ctx context.Context, key string, generation uint64) error {
	return t.guard.ExpectCheckpoint(ctx, key, generation)
}
func (t *sourcePoststageTx) Head(ctx context.Context) (model.Revision, error) {
	head, err := t.WriteTx.Head(ctx)
	if err == nil && t.staged && t.owner.mode == "head" {
		t.owner.reached++
		return model.Natural64(0), errSourcePoststage
	}
	return head, err
}
func (t *sourcePoststageTx) PutGraphRecord(ctx context.Context, row driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	if err := t.graph.PutGraphRecord(ctx, row, parent, links); err != nil {
		return err
	}
	if t.staged && t.owner.mode == "graph" {
		t.owner.reached++
		return errSourcePoststage
	}
	return nil
}
func (t *sourcePoststageTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	if t.staged && t.owner.mode == "commit" {
		t.owner.reached++
		return model.Changeset{}, errSourcePoststage
	}
	return t.WriteTx.Commit(ctx, h)
}

func TestJointSourcePublicMixedPoststageFailuresConserveBothPlanes(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			var dsn string
			store, lease, _ := openIdentityTarget(t, engine, func(s string) { dsn = s })
			p := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			observed := &sourcePoststageStore{Store: store, WorkflowCheckpointStore: cap, mode: "prime"}
			_, server := sourcePublicService(t, observed, lease, p, report, &sourcePublicPolicy{})
			cs := model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"title": "poststage active current"}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "poststage ordinary current"}}},
			}}
			// Real PostgreSQL lock placeholders are established explicitly before the
			// SQL baseline. This same request/key acquires the exact same lock set.
			sourcePublicCall(t, server, p.Admission.Actor, "tx.apply", 801, map[string]any{"changeSet": cs}, "internal_error")
			if observed.reached != 1 || observed.staged != 0 {
				t.Fatal("lock-placeholder setup did not stop before checkpoint stage")
			}
			var db *sql.DB
			if engine != "memory" {
				dialect := "sqlite"
				if engine == "postgres" {
					dialect = "pgx"
				}
				db, err = sql.Open(dialect, dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
			}
			for _, mode := range []string{"head", "graph", "commit"} {
				t.Run(mode, func(t *testing.T) {
					observed.mode, observed.staged, observed.reached = mode, 0, 0
					before := savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 801))
					if before.WriteReceipt != nil {
						t.Fatal("failure baseline already has transaction receipt")
					}
					cp, err := cap.LoadCheckpoint(ctx, report.SourcePlanes.Providers[0].Provider.Namespace)
					if err != nil {
						t.Fatal(err)
					}
					var physical map[string][]string
					if db != nil {
						physical = jointPhysicalSQL(t, db, engine)
					}
					sourcePublicCall(t, server, p.Admission.Actor, "tx.apply", 801, map[string]any{"changeSet": cs}, "internal_error")
					if observed.staged != 1 || observed.reached != 1 {
						t.Fatalf("fault not reached after actual StageCheckpoint: staged=%d reached=%d", observed.staged, observed.reached)
					}
					after := savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 801))
					current, err := cap.LoadCheckpoint(ctx, report.SourcePlanes.Providers[0].Provider.Namespace)
					if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(cp, current) {
						t.Fatal("poststage fault changed core/log/blob/receipt/checkpoint", err)
					}
					if db != nil && !reflect.DeepEqual(physical, jointPhysicalSQL(t, db, engine)) {
						t.Fatal("poststage fault changed complete physical SQL census")
					}
				})
			}
			observed.mode, observed.staged, observed.reached = "", 0, 0
			committed := sourcePublicCall(t, server, p.Admission.Actor, "tx.apply", 801, map[string]any{"changeSet": cs}, "")
			replay := sourcePublicCall(t, server, p.Admission.Actor, "tx.apply", 801, map[string]any{"changeSet": cs}, "")
			if observed.staged != 1 || !replay.Meta.Replayed || !bytes.Equal(committed.Result, replay.Result) {
				t.Fatal("same target did not publish once and replay after removing faults")
			}
		})
	}
}
