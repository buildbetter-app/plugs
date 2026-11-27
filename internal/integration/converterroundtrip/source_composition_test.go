package converterroundtrip_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestJointSourceReacquireReusesValidatedBinding(t *testing.T) {
	ctx := context.Background()
	var checkRetained atomic.Bool
	var retainedReads atomic.Int64
	var clockOffset atomic.Int64
	config := composition.Config{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "joint-recovery.db"), Project: "demo",
		RestorePresence: restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }),
		StoreFactory: func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
			store, err := (sqlite.Driver{Clock: func() time.Time {
				return time.Now().Add(time.Duration(clockOffset.Load()))
			}}).Open(ctx, config)
			if err != nil {
				return nil, err
			}
			return &reacquireSourceStore{Store: store.(*sqlite.Store), rejectRetained: &checkRetained, retainedReads: &retainedReads}, nil
		},
	}
	runtime, err := composition.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	data := converterroundtrip.JointFixtureForTest(t)
	owner := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	profile := converterroundtrip.JointProfileForTest(t, data, runtime.Store, runtime.Lease, owner)
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	request := composition.RestoreImportRequest{
		IdempotencyKey: strings.Repeat("c", 32), HistoryMode: driver.RestoreHistoryModeSnapshot,
		Presence: restore.Presence{"proof": "disposable source fixture"}, SourcePlanes: &plan,
	}
	report, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close()
	if report.SourceOriginSHA256() == "" {
		t.Fatal("import did not retain its verified immutable source digest")
	}
	ref := model.RefByID("demo-7", model.Natural64(0))
	converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "bead.update", 701, map[string]any{"id": ref, "fields": map[string]any{"title": "current checkpoint title"}})

	for _, phase := range []string{"after import", "after startup"} {
		if !runtime.Service.SourceRuntimeInstalled() {
			t.Fatalf("source provider absent %s", phase)
		}
		old := runtime
		if err := old.Lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
		checkRetained.Store(true)
		start := time.Now()
		next, err := old.Reacquire(ctx)
		if err != nil {
			t.Fatalf("reacquire %s: %v", phase, err)
		}
		runtime = next
		if elapsed := time.Since(start); elapsed >= 3*time.Second || next.Store != old.Store || next.Lease.Epoch() != old.Lease.Epoch()+1 {
			t.Fatalf("reacquire %s took %s, store reused = %t, epoch %d -> %d", phase, elapsed, next.Store == old.Store, old.Lease.Epoch(), next.Lease.Epoch())
		}
		t.Logf("source rebind %s completed in %s, epoch %d -> %d", phase, time.Since(start), old.Lease.Epoch(), next.Lease.Epoch())
		got := converterroundtrip.JointHTTPForTest(t, next.HTTPHandler, "bead.get", 0, map[string]any{"id": ref})
		if !bytes.Contains(got, []byte("current checkpoint title")) {
			t.Fatalf("rebound provider did not resolve the current source alias: %s", got)
		}
		if phase == "after import" {
			if err := next.Close(ctx); err != nil {
				t.Fatal(err)
			}
			checkRetained.Store(false)
			clockOffset.Store(int64(driver.IdempotencyWindow + time.Second))
			runtime, err = composition.Open(ctx, config)
			if err != nil {
				t.Fatalf("cold startup after import receipt replay expiry: %v", err)
			}
			meta, err := runtime.Store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := runtime.Store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			principal, err := driver.PrincipalKey(owner)
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			replay, replayErr := reader.IdempotencyByKey(ctx, principal, "", "offline.restore.import", request.IdempotencyKey)
			if err := errors.Join(replayErr, reader.Close()); err != nil || replay != nil {
				t.Fatalf("expired import receipt escaped ordinary replay TTL: exists=%v error=%v", replay != nil, err)
			}
			t.Log("cold startup installed the source workflow after its ordinary import replay expired")
		}
	}
	t.Run("tampered_advanced_checkpoint_falls_back", func(t *testing.T) {
		checkRetained.Store(false)
		store := runtime.Store.(*reacquireSourceStore)
		checkpoint, err := store.LoadCheckpoint(ctx, driver.SourcePlaneWorkflowNamespaceV1)
		if err != nil {
			t.Fatal(err)
		}
		origin, revision, logical, err := driver.ReadSourcePlaneCheckpointEnvelope(checkpoint)
		if err != nil || checkpoint.Generation <= origin.InitialGeneration {
			t.Fatalf("expected advanced checkpoint: generation=%d initial=%d error=%v", checkpoint.Generation, origin.InitialGeneration, err)
		}
		var image struct {
			Source struct {
				Records []driver.SourcePlaneRecordV1
			}
		}
		if err := json.Unmarshal(logical, &image); err != nil {
			t.Fatal(err)
		}
		var payload []byte
		for _, record := range image.Source.Records {
			changed := bytes.Replace(record.Raw, []byte("Exact active collision"), []byte("Edited retained source"), 1)
			if !bytes.Equal(changed, record.Raw) {
				original, _ := json.Marshal(record.Raw)
				replaced, _ := json.Marshal(changed)
				payload = bytes.Replace(checkpoint.Payload, original, replaced, 1)
				break
			}
		}
		if len(payload) == 0 || bytes.Equal(payload, checkpoint.Payload) {
			t.Fatal("fixture has no modified retained record")
		}
		// Save only the checkpoint. Keep its origin, revision and protected
		// commit row intact so the archive-free digest must detect the edit.
		corrupt, err := store.SaveCheckpoint(ctx, runtime.Lease, driver.SourcePlaneWorkflowNamespaceV1, checkpoint.Generation, payload)
		if err != nil {
			t.Fatal(err)
		}
		preserved, gotRevision, _, err := driver.ReadSourcePlaneCheckpointEnvelope(corrupt)
		if err != nil || preserved != origin || gotRevision != revision {
			t.Fatalf("tamper changed the envelope binding: %v", err)
		}
		if err := runtime.Lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
		before := retainedReads.Load()
		next, err := runtime.Reacquire(ctx)
		if next != nil {
			runtime = next
			t.Fatal("rebind accepted a changed retained source record")
		}
		var mismatch driver.ErrRestoreBindingMismatch
		if retainedReads.Load() <= before || !errors.As(err, &mismatch) || (mismatch.Field != "source.reverseImage" && mismatch.Field != "source.originDigest") {
			t.Fatalf("expected full retained-source recovery to refuse the checkpoint, got %v", err)
		}
	})
}

type reacquireSourceStore struct {
	*sqlite.Store
	rejectRetained *atomic.Bool
	retainedReads  *atomic.Int64
}

func (s *reacquireSourceStore) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	if driver.RetainedSourceRead(ctx) {
		s.retainedReads.Add(1)
		if s.rejectRetained.Load() {
			return nil, errors.New("reacquisition opened a retained-source recovery snapshot")
		}
	}
	return s.Store.ReadSourcePlaneSnapshot(ctx, view, keys)
}

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
			ready, err := runtime.Service.ReadyWithFilter(ctx, owner, service.ReadyRequest{Project: "demo", Limit: 1000}, nil)
			if err != nil {
				t.Fatalf("composed source Ready selected the native provider: %v", err)
			}
			matched := 0
			for _, item := range ready.Items {
				if item.ID == "workflow:item/restored" && item.Fields["title"] == "Exact active collision" && item.Kind == "workflow.item" {
					matched++
				}
			}
			if matched != 1 || ready.Next != nil || ready.WorkflowSnapshot == nil || ready.ReadyMembership == "" {
				t.Fatalf("composed Ready lost its source item or snapshot: %+v", ready)
			}
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

func TestColdSourceRecoveryAfterImportReceiptExpires(t *testing.T) {
	data := converterroundtrip.JointFixtureForTest(t)
	for _, engine := range []string{"postgres", "memory"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			clock := testclock.New(t, time.Date(2037, 3, 29, 6, 0, 0, 0, time.UTC))
			config := composition.Config{Driver: engine, Project: "demo"}
			var store driver.Store
			var err error
			if engine == "postgres" {
				config.DSN = clock.BindPostgres(t, converterroundtrip.JointPostgresDSNForTest(t))
				store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
			} else {
				store, err = memory.NewWithOptions(memory.Options{Clock: clock.Now})
				// Cold composition of a process-local store borrows the same
				// open store. The imported state lives for that store's lifetime.
				config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) {
					return store, nil
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			lease, err := store.Lease(ctx, "source-import-fixture", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			owner := model.PrincipalRef{Kind: "user", ID: "restore-admin"}
			profile := converterroundtrip.JointProfileForTest(t, data, store, lease, owner)
			report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			defer report.Close()
			if report.Receipt == nil || report.Receipt.RequestHash == "" {
				t.Fatal("source import did not publish its original receipt")
			}
			write := model.WriteMeta{Project: "demo", IdempotencyKey: strings.Repeat("d", 32)}
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			domain := service.New(store, lease, writer, service.WithOntology(ontology))
			if _, err := domain.Create(ctx, owner, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "commit after import"}}, write); err != nil {
				t.Fatal(err)
			}
			meta, err := store.Meta(ctx)
			if err != nil || meta.Head == report.Receipt.Commit.Revision {
				t.Fatalf("fixture failed to advance past the import revision: %v", err)
			}
			principal, err := driver.PrincipalKey(owner)
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err := before.IdempotencyByKey(ctx, principal, "", "bead.create", write.IdempotencyKey)
			if err := errors.Join(err, before.Close()); err != nil || ordinary == nil {
				t.Fatalf("fixture ordinary write receipt is absent before expiry: %v", err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if engine == "postgres" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(t, driver.IdempotencyWindow+time.Second)
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatalf("cold startup after import replay expiry: %v", err)
			}
			defer runtime.Close(ctx)
			if !runtime.Service.SourceRuntimeInstalled() {
				t.Fatal("startup omitted the recovered source provider")
			}
			source := model.RefByID("demo-7", model.Natural64(0))
			target, found, err := runtime.Service.ResolveSourceAddress(ctx, owner, "demo", source)
			if err != nil || !found || target.ID != "workflow:item/restored" {
				t.Fatalf("recovered source alias = %v, found=%v, error=%v", target, found, err)
			}
			reader, err := runtime.Store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			startup := driver.WithRetainedSourceRead(ctx)
			retained, err := driver.ReadNativeReverseReceipt(startup, reader, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey, nil)
			if err != nil || retained == nil || retained.RequestHash != report.Receipt.RequestHash || retained.Commit.Revision != report.Receipt.Commit.Revision {
				t.Fatalf("startup did not read the original import receipt: %v", err)
			}
			if receipt, err := reader.IdempotencyByKey(startup, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey); err != nil || receipt != nil {
				t.Fatalf("ordinary replay returned the expired import receipt: %v", err)
			}
			if receipt, err := driver.ReadNativeReverseReceipt(driver.WithNativeReverseRead(ctx), reader, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey, nil); err != nil || receipt != nil {
				t.Fatalf("offline reverse returned the expired import receipt: %v", err)
			}
			if receipt, err := driver.ReadNativeReverseReceipt(startup, reader, principal, "", "bead.create", write.IdempotencyKey, nil); err != nil || receipt != nil {
				t.Fatalf("startup returned an expired ordinary write receipt: %v", err)
			}
			t.Logf("source provider recovered at head %s from original import revision %s after 24h replay expiry", meta.Head, retained.Commit.Revision)
		})
	}
}

func TestNativeCatalogFullHistoryInstalledRPCReadyAndRestart(t *testing.T) {
	data := converterroundtrip.NativeHistoryFixtureForTest(t)
	assertNativeCatalogFullHistoryInstalledRPCReadyAndRestart(t, data, false)
}

func TestNativeTablesFullHistoryInstalledRPCReadyAndRestart(t *testing.T) {
	data := converterroundtrip.NativeTableHistoryFixtureForTest(t)
	assertNativeCatalogFullHistoryInstalledRPCReadyAndRestart(t, data, true)
}

func assertNativeCatalogFullHistoryInstalledRPCReadyAndRestart(t *testing.T, data []byte, nativeTables bool) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			dsn := ""
			if engine == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "native-catalog.db")
			}
			if engine == "postgres" {
				dsn = converterroundtrip.JointPostgresDSNForTest(t)
			}
			config := composition.Config{Driver: engine, DSN: dsn, Project: "pl", AuthMode: "anonymous", NativeImportOntology: true, RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if method != "admin.restore.begin" || len(digest) != 64 || p["proof"] != "public-native-catalog" {
					return errors.New("unbound public restore presence")
				}
				return nil
			})}
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if runtime != nil {
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				}
			}()
			plan := converterroundtrip.NativeHistoryPlanForTest(t, data, runtime.Store, runtime.Lease)
			owner := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			request := composition.RestoreImportRequest{IdempotencyKey: strings.Repeat("c", 32), HistoryMode: driver.RestoreHistoryModeFull, Presence: restore.Presence{"proof": "public-native-catalog"}, SourcePlanes: &plan}
			report, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
			if err != nil {
				t.Fatal("actual composed full native import", err)
			}
			if nativeTables && len(report.NativeFamilies) != 66 {
				t.Fatal("composed native import omitted scoped current-family accounting")
			}
			for pass := 0; pass < 2; pass++ {
				for project, category := range map[string]string{"pl": "active", "qa": "done"} {
					raw := converterroundtrip.JointHTTPForTest(t, runtime.HTTPHandler, "ontology.get", 0, map[string]any{"project": project})
					var result struct {
						Document struct {
							Statuses map[string]struct {
								Category string `json:"category"`
							} `json:"statuses"`
						} `json:"document"`
					}
					if err := json.Unmarshal(raw, &result); err != nil || result.Document.Statuses["shared"].Category != category {
						t.Fatalf("installed %s ontology.get: %s: %v", project, raw, err)
					}
					ready, err := runtime.Service.Ready(ctx, owner, service.ReadyRequest{Project: project})
					want := 0
					if category == "active" {
						want = 1
					}
					if err != nil || len(ready.Items) != want {
						t.Fatalf("installed %s live category Ready: %d, want %d: %v", project, len(ready.Items), want, err)
					}
				}
				request.SourcePlanes = nil
				recovered, err := runtime.RestoreImport(ctx, owner, bytes.NewReader(data), request)
				if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) || !reflect.DeepEqual(report.NativeFamilies, recovered.NativeFamilies) {
					t.Fatal("composed original full receipt recovery", err)
				}
				if pass == 0 && engine != "memory" {
					if err := runtime.Close(ctx); err != nil {
						t.Fatal(err)
					}
					runtime = nil
					runtime, err = composition.Open(ctx, config)
					if err != nil {
						t.Fatal("native catalog full history restart", err)
					}
				}
			}
			// Memory repeats within its process lifetime; durable drivers reopen.
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
