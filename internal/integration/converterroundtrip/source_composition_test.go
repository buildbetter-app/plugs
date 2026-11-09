package converterroundtrip_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestJointSourceCompositionImportInstallAndStartupRecovery(t *testing.T) {
	data := converterroundtrip.JointFixtureForTest(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			dsn := ""
			if engine == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "joint-composed.db")
			}
			if engine == "postgres" {
				dsn = converterroundtrip.JointPostgresDSNForTest(t)
			}
			presence := restore.Presence{"proof": "joint-composition"}
			config := composition.Config{Driver: engine, DSN: dsn, Project: "demo", AuthMode: "anonymous", RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if method != "admin.restore.begin" || len(digest) != 64 || p["proof"] != "joint-composition" {
					return errors.New("unbound presence")
				}
				return nil
			})}
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if runtime != nil {
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				}
			})
			owner := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			profile := converterroundtrip.JointProfileForTest(t, data, runtime.Store, runtime.Lease, owner)
			plan, err := profile.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			request := composition.RestoreImportRequest{IdempotencyKey: strings.Repeat("c", 32), HistoryMode: driver.RestoreHistoryModeSnapshot, Presence: presence, SourcePlanes: &plan}
			report, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil {
				t.Fatal("composed joint import", err)
			}
			if !runtime.Service.SourceRuntimeInstalled() {
				t.Fatal("import omitted live installation")
			}
			admitted, original, found, err := converterroundtrip.RecoverBoundSourceImport(ctx, runtime.Store)
			if err != nil || !found || !reflect.DeepEqual(original.Receipt, report.Receipt) || !reflect.DeepEqual(admitted.Admission.Actor, owner) {
				t.Fatal("composition did not retain the actual admitted importer and original receipt", err)
			}
			ref := model.RefByID("demo-7", model.Natural64(0))
			got := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
			if !bytes.Contains(got, []byte("Exact active collision")) {
				t.Fatal("bound HTTP handler missed newly installed provider", string(got))
			}
			converterroundtrip.JointAssertSourceCommentForTest(t, got, admitted, report)
			for i, test := range []struct{ target, code string }{{"demo-7", "unsupported"}, {"demo-8", "ontology_guard_failed"}} {
				// The composed runtime also owns a native default provider. A delete
				// must not replace the installed source participant with that provider,
				// even when a durable row has the same naked ID as an active source.
				key := strings.Repeat(string(rune('d'+i)), 32)
				before := sourceCompositionDeleteCensus(t, runtime.Store, owner, key)
				change := model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": "discard source delete draft"}}},
					{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: model.RefByID(test.target, model.Natural64(0))}},
				}}
				_, err := runtime.Service.ApplyWithOutcome(ctx, owner, change, model.WriteMeta{Project: "demo", AgentLabel: owner.ID, IdempotencyKey: key})
				var refused *model.Error
				if !errors.As(err, &refused) || string(refused.Data.Code) != test.code {
					t.Fatalf("delete bypassed the source participant for %s: %v", test.target, err)
				}
				if !reflect.DeepEqual(before, sourceCompositionDeleteCensus(t, runtime.Store, owner, key)) {
					t.Fatal("refused source delete changed core, checkpoint, or receipt")
				}
			}
			created, err := runtime.Service.Create(ctx, owner, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "ordinary source-path delete"}}, model.WriteMeta{Project: "demo", AgentLabel: owner.ID, IdempotencyKey: strings.Repeat("f", 32)})
			if err != nil || len(created.Results) != 1 {
				t.Fatalf("create ordinary delete control: %v", err)
			}
			ordinary := created.Results[0].Ref
			change := model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ordinary}}}}
			deleteMeta := model.WriteMeta{Project: "demo", AgentLabel: owner.ID, IdempotencyKey: strings.Repeat("a", 32)}
			deleted, err := runtime.Service.ApplyWithOutcome(ctx, owner, change, deleteMeta)
			if err != nil {
				t.Fatalf("ordinary source-path delete regressed: %v", err)
			}
			replayed, err := runtime.Service.ApplyWithOutcome(ctx, owner, change, deleteMeta)
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(deleted.Commit, replayed.Commit) {
				t.Fatalf("ordinary source-path delete lost canonical replay: %v", err)
			}
			_, err = runtime.Service.Get(ctx, owner, service.GetRequest{ID: ordinary, Project: "demo"})
			var missing *model.Error
			if !errors.As(err, &missing) || missing.Data.Code != "not_found" {
				t.Fatalf("ordinary source-path delete did not remove its target: %v", err)
			}
			converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "composed current source title"}})
			request.SourcePlanes = nil
			recovered, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
				t.Fatal("composed import did not recover original receipt", err)
			}
			if engine != "memory" {
				if err := runtime.Close(ctx); err != nil {
					t.Fatal(err)
				}
				runtime = nil
				runtime, err = composition.Open(ctx, config)
				if err != nil {
					t.Fatal("startup recovery", err)
				}
			}
			if !runtime.Service.SourceRuntimeInstalled() {
				t.Fatal("source provider absent after lifecycle boundary")
			}
			current := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
			if !bytes.Contains(current, []byte("composed current source title")) {
				t.Fatal("recovery substituted initial receipt payload for current namespace", string(current))
			}
			converterroundtrip.JointAssertSourceCommentForTest(t, current, admitted, report)
			replay := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "composed current source title"}})
			if !bytes.Contains(replay, []byte("composed current source title")) {
				t.Fatal("recovery replay changed result", string(replay))
			}
			// Memory retained this one process-local store; only persistent stores were
			// closed and reopened. No restart persistence is claimed for memory.
		})
	}
}

func sourceCompositionDeleteCensus(t *testing.T, store driver.Store, owner model.PrincipalRef, key string) any {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := store.(driver.CheckpointInventoryStore).ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer inventory.Close()
	rows, err := inventory.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil || len(rows.Items) == 1000 {
		t.Fatalf("incomplete source delete core census: %v", err)
	}
	keys, err := inventory.CheckpointKeys()
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := make([]driver.Checkpoint, 0, len(keys))
	for _, key := range keys {
		checkpoint, err := inventory.SourcePlaneCheckpoint(key)
		if err != nil {
			t.Fatal(err)
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	principal, err := driver.PrincipalKey(owner)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := inventory.IdempotencyByKey(ctx, principal, owner.ID, "tx.apply", key)
	if err != nil || receipt != nil {
		t.Fatalf("refused source delete retained a receipt: %v", err)
	}
	return struct {
		Meta        driver.Meta
		Records     []driver.Record
		Checkpoints []driver.Checkpoint
	}{meta, rows.Items, checkpoints}
}
