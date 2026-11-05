package postgresacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type b21PrivateTarget struct {
	store  driver.Store
	lease  driver.ActiveLease
	cap    driver.WorkflowCheckpointStore
	db     *sql.DB
	engine string
	open   func() driver.Store
}

func TestB21SourceReverseImageCurrentSnapshotWithoutPublication(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			original, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, original, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			initial, err := workflowplugin.InspectSourceReverseImage(logical)
			if err != nil || initial.OriginSHA256 == "" {
				t.Fatalf("reverse admitted-origin precondition: %v", err)
			}
			tx, _, prepared, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			p := b21Runtime(t, v, prepared.Binding)
			parent := ref("workflow:item/source-parent")
			if _, err := p.Update(ctx, b21SourceActor, parent, map[string]any{"title": "current reverse title"}, b21WriteMeta("reverse-update", "bead.update")); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Comment(ctx, b21SourceActor, parent, "authenticated current comment", b21WriteMeta("reverse-comment", "bead.comment")); err != nil {
				t.Fatal(err)
			}
			// Keep a stored claim whose expiry precedes the inspection clock. A
			// snapshot accessor must preserve it without clock-based projection.
			p.WithClock(func() time.Time { return clock.Add(-2 * workflowplugin.ClaimTTL) })
			claimed, err := p.Claim(ctx, b21SourceActor, parent, b21WriteMeta("reverse-claim", "bead.claim"))
			if err != nil || claimed.Claim == nil || !claimed.Claim.ExpiresAt.Before(clock) {
				t.Fatalf("expired stored claim precondition: %+v %v", claimed, err)
			}
			p.WithClock(func() time.Time { return clock })
			read := func() workflowplugin.SourceReverseImage {
				t.Helper()
				before := v.physical(t)
				snapshot, err := v.cap.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: prepared.Binding.Origin.Incarnation}, []string{prepared.Mutation.Key})
				if err != nil {
					t.Fatal(err)
				}
				// This is the same pinned core/provider observation. No ordinary
				// writer Head is held and no original Provider is reloaded.
				core, err := snapshot.Query(ctx, driver.Query{Limit: 1000})
				if err != nil || core.NextCursor != nil {
					t.Fatalf("reverse pinned core census: %v", err)
				}
				for _, row := range core.Items {
					if strings.HasPrefix(row.Ref.ID, workflowplugin.ItemPrefix) {
						t.Fatal("source item entered the ordinary core")
					}
				}
				checkpoint, err := snapshot.SourcePlaneCheckpoint(prepared.Mutation.Key)
				if err != nil || checkpoint.Generation <= prepared.Binding.Origin.InitialGeneration {
					t.Fatalf("reverse pinned current checkpoint: %+v %v", checkpoint, err)
				}
				host := pluginhost.NewDurableState(v.cap, v.lease, "workflow", "default").(driver.SourcePlaneOriginReader)
				origin, current, err := host.InspectSourcePlaneOrigin(checkpoint)
				if err != nil || origin != prepared.Binding.Origin {
					t.Fatalf("reverse pinned immutable origin: %+v %v", origin, err)
				}
				copy := bytes.Clone(current)
				image, err := workflowplugin.InspectSourceReverseImage(current)
				if err != nil || !bytes.Equal(copy, current) {
					t.Fatalf("reverse current logical inspection: %v", err)
				}
				if image.OriginSHA256 != initial.OriginSHA256 {
					t.Fatal("supported current mutation or terminal cleanup changed the admitted immutable origin")
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, v.physical(t)) {
					t.Fatal("reverse inspection changed graph, receipts, checkpoint generation or physical rows")
				}
				return image
			}
			parentKey := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: "same-id"}
			childKey := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: "child"}
			image := read()
			entry := image.Entities[parentKey]
			if len(image.Entities) != 2 || len(image.Relations) != 3 || !entry.Present || entry.Terminal || entry.Item == nil || entry.Item.Title != "current reverse title" || entry.Item.Metadata["large"] != json.Number("9007199254740993") || entry.Item.Claim == nil || !reflect.DeepEqual(entry.Item.Claim, claimed.Claim) {
				t.Fatalf("inspection returned stale or projected current state: %+v", image)
			}
			comments := entry.Item.Comments
			if len(comments) != 2 || comments[0].Source == nil || comments[0].Source.Author != "unverified-author" || comments[0].Principal.Kind != "" || comments[1].Source != nil || !reflect.DeepEqual(comments[1].Principal, b21SourceActor) {
				t.Fatalf("inspection changed imported/authenticated comment attribution: %+v", comments)
			}
			if _, err := p.Close(ctx, b21SourceActor, parent, "complete", b21WriteMeta("reverse-close", "bead.close")); err != nil {
				t.Fatal(err)
			}
			terminal := read()
			entry = terminal.Entities[parentKey]
			if len(terminal.Entities) != 2 || entry.Present || !entry.Terminal || entry.Item != nil || entry.TargetID != parent.ID || !terminal.Entities[childKey].Present || len(terminal.Relations) != 0 {
				t.Fatalf("terminal source identity revived old payload or relations: %+v", terminal)
			}
		})
	}
}

func b21Target(t *testing.T, engine string) *b21PrivateTarget {
	t.Helper()
	ctx := context.Background()
	v := &b21PrivateTarget{engine: engine}
	switch engine {
	case "memory":
		v.open = func() driver.Store { return memory.New() }
	case "sqlite":
		path := filepath.Join(t.TempDir(), "source-runtime.db")
		v.open = func() driver.Store {
			s, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		var err error
		v.db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
	case "postgres":
		dsn := freshDatabase(t)
		v.open = func() driver.Store { return openStore(t, dsn) }
		var err error
		v.db, err = sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown source runtime engine")
	}
	v.store = v.open()
	v.acquire(t)
	t.Cleanup(func() {
		if v.lease != nil {
			_ = v.lease.Release(context.Background())
		}
		if v.store != nil {
			_ = v.store.Close()
		}
		if v.db != nil {
			_ = v.db.Close()
		}
	})
	// PostgreSQL prepares lock placeholder rows on its reserved connection.
	// Establish the private fixture's exact lock census before any physical
	// conservation baseline; every table, including locks, remains compared.
	for _, id := range []string{"pl-source-shadow", "pl-source-intervening"} {
		tx, err := v.store.Write(ctx, v.lease)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Lock(ctx, b21CoreLocks(t, ref(id))); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		_ = tx.Close()
	}
	return v
}

func b21CoreLocks(t *testing.T, id model.Ref) []driver.LockRequest {
	t.Helper()
	bead, err := driver.NewBeadSentinel(id.ID)
	if err != nil {
		t.Fatal(err)
	}
	return []driver.LockRequest{
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &id}, Mode: driver.LockUpdate},
		{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead}, Mode: driver.LockUpdate},
	}
}

// Workflow addresses have no ordinary bead sentinels. Observe the complete
// ordinary graph, then independently inspect the actual typed version keys.
func b21CoreImage(t *testing.T, store driver.Store) graphState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := reader(t, store)
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("source core census: %+v %v", page, err)
	}
	var after *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		after = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	sets, err := r.Changesets(ctx, after, 1000)
	if err != nil || len(sets) == 1000 {
		t.Fatalf("retained source core history: %v", err)
	}
	if after != nil {
		// Current-image restore has floor 1. The log API correctly refuses a
		// cursor below that floor; retain the actual base entry via its receipt.
		principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "system", ID: "plug-converter"})
		if err != nil {
			t.Fatal(err)
		}
		base, err := r.ChangesetByKey(ctx, principal, "", "offline.restore", key("b21-private-profile"))
		if err != nil || base == nil || meta.HistoryFloor.String() != "1" || base.Revision.String() != "1" || base.Kind != model.ChangesetRestore {
			t.Fatalf("source restore base receipt: %+v %v", base, err)
		}
		sets = append([]model.Changeset{*base}, sets...)
	}
	state := graphState{Meta: meta, Records: page.Items, Changesets: sets, Versions: map[string][]driver.Version{}}
	names := []string{"workflow:item/source-parent", "workflow:item/source-child"}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	for _, row := range page.Items {
		names = append(names, row.Ref.ID)
		id := row.Ref
		bead, err := driver.NewBeadSentinel(id.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &id}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	for _, id := range names {
		page, err := r.Versions(ctx, ref(id), driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("active versions %s: %+v %v", id, page, err)
		}
		state.Versions[id] = page.Items
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	state.Observations, err = r.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func (v *b21PrivateTarget) acquire(t *testing.T) {
	t.Helper()
	var err error
	v.lease, err = v.store.Lease(context.Background(), "b21-private-provider", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	v.cap, err = driver.RequireWorkflowCheckpointStore(v.store, v.lease)
	if err != nil {
		t.Fatal(err)
	}
}

func (v *b21PrivateTarget) reopen(t *testing.T) {
	t.Helper()
	if err := v.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	v.lease = nil
	if v.engine != "memory" {
		if err := v.store.Close(); err != nil {
			t.Fatal(err)
		}
		v.store = v.open()
	}
	v.acquire(t)
}

func (v *b21PrivateTarget) physical(t *testing.T) any {
	t.Helper()
	if v.db != nil {
		return b21CompleteSQL(t, v.db, v.engine)
	}
	cp, err := v.cap.LoadCheckpoint(context.Background(), "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return struct {
		Graph      graphState
		Checkpoint driver.Checkpoint
	}{b21CoreImage(t, v.store), cp}
}

func b21Profile(t *testing.T, f b21SourceFixture) driver.RestoreImportProfile {
	t.Helper()
	binding, err := driver.NewSourcePlaneImportBindingV1(f.plan)
	if err != nil {
		t.Fatal(err)
	}
	return driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: b21SourceActor, Authority: "restore-admin", AdmissionDigest: strings.Repeat("b", 64)}, SourceStoreID: driver.StoreID(f.plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: key("b21-private-profile"), ArchiveDigest: f.plan.ArchiveSHA256, DerivedDigest: strings.Repeat("c", 64), SourcePlanes: &binding}
}

// This uses the real private OfflineRestoreTx to exercise the host participant
// in the same transaction as new Meta. It deliberately does not call the
// still-core-only public joint importer, or claim accepted archive evidence.
func b21PreparePrivateRestore(t *testing.T, v *b21PrivateTarget, f b21SourceFixture, logical []byte) (driver.WriteTx, driver.Meta, driver.PreparedSourcePlaneCheckpoint, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	tx, err := v.store.Write(ctx, v.lease)
	if err != nil {
		t.Fatal(err)
	}
	id := ref("pl-source-shadow")
	locks := b21CoreLocks(t, id)
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	meta, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, driver.StoreID(f.plan.SourceStoreID))
	if err != nil {
		t.Fatal(err)
	}
	profile := b21Profile(t, f)
	host := pluginhost.NewDurableState(v.cap, v.lease, "workflow", "default").(driver.SourcePlaneCheckpointPreparer)
	prepared, err := host.PrepareRestoreCheckpoint(ctx, meta, profile, f.plan.Providers[0], logical)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Mutation.StoreID != meta.StoreID || prepared.Mutation.Incarnation != meta.Incarnation || prepared.Mutation.Expected != 0 {
		t.Fatal("checkpoint participant lost new Meta")
	}
	if err := tx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, prepared.Mutation.Key, prepared.Mutation.Expected, prepared.Mutation.Payload); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"pl-source-shadow","type":"task","project":"` + project + `","fields":{"title":"durable shadow","status":"open"}}`)
	if err := tx.Put(ctx, driver.Record{Ref: id, Kind: "bead", Body: body}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{locks[1].Key.Sentinel}); err != nil {
		t.Fatal(err)
	}
	hash, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	header := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: clock, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: profile.IdempotencyKey, RequestHash: hash, Response: model.Commit{Revision: meta.Head, Results: []model.OpResult{}, Warnings: []model.Warning{}}, Changes: []model.ChangeRecord{{Ref: id, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}, LockKeys: []driver.LockKey{locks[0].Key, locks[1].Key}}
	return tx, meta, prepared, header
}

func b21Runtime(t *testing.T, v *b21PrivateTarget, binding driver.SourcePlaneProviderCommitV1) *workflowplugin.Provider {
	t.Helper()
	state, err := pluginhost.NewSourceRuntimeState(context.Background(), v.cap, v.lease, binding)
	if err != nil {
		t.Fatal(err)
	}
	if state.(interface{ Persistent() bool }).Persistent() != (v.engine != "memory") {
		t.Fatal("source runtime changed truthful persistence")
	}
	p, err := workflowplugin.New(state)
	if err != nil {
		t.Fatal(err)
	}
	return p.WithClock(func() time.Time { return clock })
}

func b21WriteMeta(label, method string) model.WriteMeta {
	return model.WriteMeta{Project: project, Method: method, IdempotencyKey: key(label)}
}

func b21StageNoop(t *testing.T, v *b21PrivateTarget, prepared workflowplugin.PreparedState) error {
	t.Helper()
	ctx := context.Background()
	tx, err := v.store.Write(ctx, v.lease)
	if err != nil {
		return err
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, nil); err != nil {
		return err
	}
	head, err := tx.Head(ctx)
	if err != nil {
		return err
	}
	return b21CommitPreparedAtHead(t, v, tx, head, prepared)
}

func b21CommitPreparedAtHead(t *testing.T, v *b21PrivateTarget, tx driver.WriteTx, head model.Revision, prepared workflowplugin.PreparedState) error {
	t.Helper()
	ctx := context.Background()
	meta, err := v.store.Meta(ctx)
	if err != nil {
		return err
	}
	observed, ok := prepared.CoreSnapshot()
	if !ok || observed.Head.String() != head.String() || observed.Check(meta) != nil {
		return driver.ErrCheckpointConflict
	}
	mutation, err := prepared.Checkpoint(ctx)
	if err != nil {
		return err
	}
	if mutation.StoreID != meta.StoreID || mutation.Incarnation != meta.Incarnation {
		return driver.ErrCheckpointConflict
	}
	if err = tx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, mutation.Key, mutation.Expected, mutation.Payload); err != nil {
		return err
	}
	_, err = tx.(driver.NoopCommitter).CommitNoop(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Time: clock, Principal: b21SourceActor, Method: "private.source.batch", IdempotencyKey: key("b21-private-staged-batch"), RequestHash: key("b21-private-staged-request"), Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}})
	return err
}

func TestB21SourceRuntimePrivateAtomicCheckpointsThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			original, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, original, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			before := v.physical(t)
			tx, _, _, _ := b21PreparePrivateRestore(t, v, f, logical)
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			if !reflect.DeepEqual(before, v.physical(t)) {
				t.Fatal("rolled-back source checkpoint/new Meta/core changed physical store")
			}
			tx, meta, prepared, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			actual, err := v.store.Meta(ctx)
			if err != nil || actual != meta {
				t.Fatalf("new Meta publication=%+v %v", actual, err)
			}
			cp, err := v.cap.LoadCheckpoint(ctx, prepared.Mutation.Key)
			if err != nil || cp.Generation != 1 || !bytes.Equal(cp.Payload, prepared.Mutation.Payload) {
				t.Fatalf("checkpoint atomic publication=%+v %v", cp, err)
			}
			origin, stored, err := pluginhost.NewDurableState(v.cap, v.lease, "workflow", "default").(driver.SourcePlaneOriginReader).InspectSourcePlaneOrigin(cp)
			if err != nil || origin != prepared.Binding.Origin || !bytes.Equal(stored, logical) {
				t.Fatalf("stored origin/image=%v", err)
			}
			if _, err := pluginhost.NewSourceRuntimeState(ctx, v.cap, v.lease, prepared.Binding); err != nil {
				t.Fatal(err)
			}
			legacy := pluginhost.NewDurableState(v.cap, v.lease, "workflow", "default")
			var unused any
			if _, _, err := legacy.Read(ctx, "workflow/state", &unused); err == nil {
				t.Fatal("legacy unbound facade read source namespace")
			}
			p := b21Runtime(t, v, prepared.Binding)
			parent, child := ref("workflow:item/source-parent"), ref("workflow:item/source-child")
			view, err := p.Get(ctx, b21SourceActor, parent, b21WriteMeta("get", "bead.get"))
			if err != nil || view.ID != parent.ID || len(view.Comments) != 1 || view.Comments[0].Source == nil || view.ChildCounter != 13 {
				t.Fatalf("restored active read=%+v %v", view, err)
			}
			resolved, ok, err := p.ResolveSourceRef(ctx, b21SourceActor, project, "same-id", b21WriteMeta("resolve", "bead.get"))
			if err != nil || !ok || resolved != parent {
				t.Fatalf("active-first physical collision=%+v %v", resolved, err)
			}
			ready, err := p.Ready(ctx, b21SourceActor, project, nil, b21WriteMeta("ready", "bead.ready"), true)
			if err != nil || len(ready) != 1 || ready[0].ID != parent.ID {
				t.Fatalf("same-image dependency readiness=%+v %v", ready, err)
			}
			frozen := v.physical(t)
			if _, err := p.Claim(ctx, b21SourceActor, child, b21WriteMeta("blocked-claim", "bead.claim")); err == nil {
				t.Fatal("blocked direct claim accepted")
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("refused direct claim changed complete physical image")
			}
			if _, err := p.Invoke(ctx, b21SourceActor, workflowplugin.InvokeRequest{Template: "new", Project: project, Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "new", Title: "new", Type: "task"}}}, b21WriteMeta("invoke", "x.workflow.invoke")); err == nil {
				t.Fatal("restored process capability granted ordinary Invoke")
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("refused Invoke changed complete physical image")
			}
			preparedBatch, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				if _, err := staged.Update(ctx, b21SourceActor, parent, map[string]any{"title": "ordered title"}, b21WriteMeta("batch-update", "bead.update")); err != nil {
					return err
				}
				_, err := staged.Comment(ctx, b21SourceActor, parent, "ordered comment", b21WriteMeta("batch-comment", "comment.add"))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("preparation published before atomic writer")
			}
			if err := b21StageNoop(t, v, preparedBatch); err != nil {
				t.Fatal(err)
			}
			updated, err := p.Get(ctx, b21SourceActor, parent, b21WriteMeta("after-batch", "bead.get"))
			if err != nil || updated.Title != "ordered title" || len(updated.Comments) != 2 || updated.Metadata["large"] != json.Number("9007199254740993") {
				t.Fatalf("ordered checkpoint batch=%+v %v", updated, err)
			}
			checkpoint, err := v.cap.LoadCheckpoint(ctx, prepared.Mutation.Key)
			if err != nil || checkpoint.Generation != 2 {
				t.Fatalf("batch was not one namespace publication: %v %+v", err, checkpoint)
			}
			currentOrigin, _, err := legacy.(driver.SourcePlaneOriginReader).InspectSourcePlaneOrigin(checkpoint)
			if err != nil || currentOrigin != origin {
				t.Fatal("legitimate mutation replaced immutable origin")
			}
			frozen = v.physical(t)
			if _, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				if _, err := staged.Update(ctx, b21SourceActor, parent, map[string]any{"title": "discarded"}, b21WriteMeta("bad-batch-update", "bead.update")); err != nil {
					return err
				}
				_, err := staged.Comment(ctx, model.PrincipalRef{Kind: "user", ID: "other"}, parent, "forbidden", b21WriteMeta("bad-batch-comment", "comment.add"))
				return err
			}); err == nil {
				t.Fatal("failed ordered batch returned prepared state")
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("later operation refusal published earlier active operation")
			}
			if _, err := p.Update(ctx, b21SourceActor, parent, map[string]any{"title": "ordered title"}, b21WriteMeta("batch-update", "bead.update")); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("receipt replay changed physical store")
			}
			// SQL closes/reopens its actual file/database. Memory retains only the
			// same live Store; a distinct Store cannot load this process state.
			v.reopen(t)
			p = b21Runtime(t, v, prepared.Binding)
			if reopened, err := p.Get(ctx, b21SourceActor, parent, b21WriteMeta("reopen", "bead.get")); err != nil || reopened.Title != "ordered title" {
				t.Fatalf("lifetime readback=%+v %v", reopened, err)
			}
			if _, err := p.Close(ctx, b21SourceActor, parent, "complete", b21WriteMeta("close-parent", "bead.close")); err != nil {
				t.Fatal(err)
			}
			resolved, ok, err = p.ResolveSourceRef(ctx, b21SourceActor, project, "same-id", b21WriteMeta("fallback", "bead.get"))
			if err != nil || !ok || resolved.ID != "pl-source-shadow" {
				t.Fatalf("declared collision fallback=%+v %v", resolved, err)
			}
			if _, err := p.Claim(ctx, b21SourceActor, child, b21WriteMeta("ready-claim", "bead.claim")); err != nil {
				t.Fatal(err)
			}
			raw, err := p.SourceRecords(ctx)
			if err != nil || len(raw) != len(f.raw) {
				t.Fatal("source raw origin retention changed")
			}
			state := b21CoreImage(t, v.store)
			if state.Meta.Head.String() != "1" || len(state.Changesets) != 1 || len(state.Versions[parent.ID]) != 0 || len(state.Versions[child.ID]) != 0 {
				t.Fatalf("active operations created core history: %+v", state)
			}
			if engine == "memory" {
				fresh := b21Target(t, "memory")
				if _, err := pluginhost.NewSourceRuntimeState(ctx, fresh.cap, fresh.lease, prepared.Binding); err == nil {
					t.Fatal("process namespace leaked to distinct store")
				}
			}
		})
	}
}

func TestB21HostSourceCheckpointPreparationRefusesWithoutPublication(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			current, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, current, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			profile := b21Profile(t, f)
			meta, err := driver.NewOfflineRestoreMeta(current, profile.SourceStoreID)
			if err != nil {
				t.Fatal(err)
			}
			host := pluginhost.NewDurableState(v.cap, v.lease, "workflow", "default").(driver.SourcePlaneCheckpointPreparer)
			frozen := v.physical(t)
			valid, err := host.PrepareRestoreCheckpoint(ctx, meta, profile, f.plan.Providers[0], logical)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("data-only host preparation published")
			}
			for name, change := range map[string]func(*driver.Meta, *driver.SourcePlaneProviderV1, *[]byte){
				"old incarnation":       func(m *driver.Meta, _ *driver.SourcePlaneProviderV1, _ *[]byte) { m.Incarnation = current.Incarnation },
				"wrong source identity": func(m *driver.Meta, _ *driver.SourcePlaneProviderV1, _ *[]byte) { m.StoreID = "different-source" },
				"wrong new head":        func(m *driver.Meta, _ *driver.SourcePlaneProviderV1, _ *[]byte) { m.Head = model.Natural64(2) },
				"wrong image size":      func(_ *driver.Meta, p *driver.SourcePlaneProviderV1, _ *[]byte) { p.LogicalImageSize++ },
				"wrong schema":          func(_ *driver.Meta, p *driver.SourcePlaneProviderV1, _ *[]byte) { p.Schema = "unsupported" },
				"wrong namespace": func(_ *driver.Meta, p *driver.SourcePlaneProviderV1, _ *[]byte) {
					p.Namespace = "plugin/workflow/other/durable"
				},
				"wrong image": func(_ *driver.Meta, _ *driver.SourcePlaneProviderV1, b *[]byte) { *b = []byte(`{}`) },
			} {
				t.Run(name, func(t *testing.T) {
					m, p, b := meta, f.plan.Providers[0], bytes.Clone(logical)
					change(&m, &p, &b)
					prepared, err := host.PrepareRestoreCheckpoint(ctx, m, profile, p, b)
					if err == nil || len(prepared.Mutation.Payload) != 0 {
						t.Fatalf("invalid preparation produced participant: %v", err)
					}
					if !reflect.DeepEqual(frozen, v.physical(t)) {
						t.Fatal("refused host preparation changed complete physical image")
					}
				})
			}
			// The prepared mutation owns its bytes independently of logical input.
			envelopeBefore := bytes.Clone(valid.Mutation.Payload)
			logical[0] = '!'
			if !bytes.Equal(valid.Mutation.Payload, envelopeBefore) || !json.Valid(valid.Mutation.Payload) {
				t.Fatal("prepared envelope aliases logical input")
			}
		})
	}
}

func TestB21SourcePreparedNamespaceConflictConservesAllStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			meta, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, meta, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			tx, _, initial, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			p := b21Runtime(t, v, initial.Binding)
			parent := ref("workflow:item/source-parent")
			pending, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				_, err := staged.Update(ctx, b21SourceActor, parent, map[string]any{"title": "stale"}, b21WriteMeta("stale", "bead.update"))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Update(ctx, b21SourceActor, parent, map[string]any{"title": "winner"}, b21WriteMeta("winner", "bead.update")); err != nil {
				t.Fatal(err)
			}
			before := v.physical(t)
			if err := b21StageNoop(t, v, pending); !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatalf("stale namespace participant=%v", err)
			}
			if !reflect.DeepEqual(before, v.physical(t)) {
				t.Fatal("stale source batch changed physical store")
			}
			view, err := p.Get(ctx, b21SourceActor, parent, b21WriteMeta("winner-get", "bead.get"))
			if err != nil || view.Title != "winner" {
				t.Fatal("stale participant replaced winner")
			}
		})
	}
}

func TestB21SourceStagedParentGuardsUnderHeldHeadThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			meta, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, meta, v.cap.WorkflowCheckpointLifetime())
			b21ChangeEdge(t, &f, 0, "parent-child")
			logical := b21Logical(t, &f)
			restore, _, initial, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := restore.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = restore.Close()
			p := b21Runtime(t, v, initial.Binding)
			var tx driver.WriteTx
			var heldHead model.Revision
			defer func() {
				if tx != nil {
					_ = tx.Close()
				}
			}()
			pending, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				// The source image and both guards are pinned before this Head.
				// Any nested SQL snapshot writer here would block on this writer.
				var err error
				tx, err = v.store.Write(ctx, v.lease)
				if err != nil {
					return err
				}
				if _, err := tx.Lock(ctx, nil); err != nil {
					return err
				}
				heldHead, err = tx.Head(ctx)
				if err != nil {
					return err
				}
				ids := []string{"pl-source-shadow"}
				meta := b21WriteMeta("staged-parent-guard", "bead.claim")
				facts, first, err := staged.OrdinaryParentSnapshot(ctx, b21SourceActor, ids, meta, nil)
				if err != nil {
					return err
				}
				fact := facts[ids[0]]
				if fact.Parent == nil || fact.Parent.ID != "workflow:item/source-parent" || fact.Blocked || first.Key != initial.Mutation.Key || first.Expected != 1 || first.Incarnation != initial.Binding.Origin.Incarnation {
					t.Fatalf("initial staged parent/guard: %+v %+v", fact, first)
				}
				if _, err := staged.Update(ctx, b21SourceActor, ref("workflow:item/source-parent"), map[string]any{"blocked": true}, b21WriteMeta("stage-parent-blocked", "bead.update")); err != nil {
					return err
				}
				facts, after, err := staged.OrdinaryParentSnapshot(ctx, b21SourceActor, ids, meta, nil)
				if err != nil {
					return err
				}
				if !facts[ids[0]].Blocked || after != first {
					t.Fatalf("ordered staged decision lost original guard: %+v %+v", facts, after)
				}
				_, blocker, err := staged.OrdinaryBlockerSnapshot(ctx, b21SourceActor, ids, meta)
				if err != nil {
					return err
				}
				if blocker != first {
					t.Fatal("staged ordinary guards consumed different namespace generations")
				}
				return tx.(driver.CheckpointGuardTx).ExpectCheckpoint(ctx, first.Key, first.Expected)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := b21CommitPreparedAtHead(t, v, tx, heldHead, pending); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			tx = nil
			facts, _, err := p.OrdinaryParentSnapshot(ctx, b21SourceActor, []string{"pl-source-shadow"}, b21WriteMeta("after-stage-parent", "bead.ready"), nil)
			if err != nil || !facts["pl-source-shadow"].Blocked {
				t.Fatalf("published staged parent guard: %+v %v", facts, err)
			}
			checkpoint, err := v.cap.LoadCheckpoint(ctx, initial.Mutation.Key)
			if err != nil || checkpoint.Generation != 2 {
				t.Fatalf("guarded source stage was not one publication: %+v %v", checkpoint, err)
			}
			state := b21CoreImage(t, v.store)
			if state.Meta.Head.String() != "1" || len(state.Changesets) != 1 {
				t.Fatal("source-only guarded stage created core history")
			}
			before := v.physical(t)
			readOnly, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				facts, expectation, err := staged.OrdinaryParentSnapshot(ctx, b21SourceActor, []string{"pl-source-shadow"}, b21WriteMeta("read-only-staged-parent", "bead.ready"), nil)
				if err == nil && (!facts["pl-source-shadow"].Blocked || expectation.Expected != 2) {
					t.Fatal("read-only staged guard lost the actual current namespace")
				}
				return err
			})
			if err != nil || !readOnly.Empty() {
				t.Fatalf("read-only staged guard prepared a write: %v", err)
			}
			observed, ok := readOnly.CoreSnapshot()
			if !ok || observed.Check(state.Meta) != nil {
				t.Fatalf("read-only staged guard lost its consumed core image: %+v", observed)
			}
			if _, err := readOnly.Checkpoint(ctx); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("read-only staged decision produced a checkpoint: %v", err)
			}
			if !reflect.DeepEqual(before, v.physical(t)) {
				t.Fatal("read-only staged guard changed the physical store")
			}
		})
	}
}

func TestB21SourcePreparedHeadConflictConservesAllStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			meta, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, meta, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			tx, _, initial, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			p := b21Runtime(t, v, initial.Binding)
			pending, err := p.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
				_, err := staged.Update(ctx, b21SourceActor, ref("workflow:item/source-parent"), map[string]any{"title": "stale head"}, b21WriteMeta("head-stale", "bead.update"))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			tx, err = v.store.Write(ctx, v.lease)
			if err != nil {
				t.Fatal(err)
			}
			id := ref("pl-source-intervening")
			locks := b21CoreLocks(t, id)
			if _, err := tx.Lock(ctx, locks); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put(ctx, driver.Record{Ref: id, Kind: "bead", Body: []byte(`{"id":"pl-source-intervening","type":"task","fields":{"title":"intervening","status":"open"}}`)}); err != nil {
				t.Fatal(err)
			}
			if err := tx.BumpSentinels(ctx, []driver.Sentinel{locks[1].Key.Sentinel}); err != nil {
				t.Fatal(err)
			}
			header = driver.ChangesetHeader{Incarnation: initial.Binding.Origin.Incarnation, Kind: model.ChangesetWrite, Time: clock, Principal: b21SourceActor, Method: "private.ordinary.create", IdempotencyKey: key("head-winner"), RequestHash: key("head-winner-request"), LockKeys: []driver.LockKey{locks[0].Key, locks[1].Key}, Changes: []model.ChangeRecord{{Ref: id, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}}
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			before := v.physical(t)
			if err := b21StageNoop(t, v, pending); !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatalf("stale core participant=%v", err)
			}
			if !reflect.DeepEqual(before, v.physical(t)) {
				t.Fatal("stale graph-dependent batch changed physical store")
			}
		})
	}
}

// This is host admission/callback evidence over private restored test input.
// It does not install a canonical service route or qualify a native recording.
func TestB21SourceHostAdmissionRetainsInvocationFence(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			v := b21Target(t, engine)
			meta, err := v.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			f := b21Fixture(t, meta, v.cap.WorkflowCheckpointLifetime())
			logical := b21Logical(t, &f)
			tx, _, prepared, header := b21PreparePrivateRestore(t, v, f, logical)
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			_ = tx.Close()
			frozen := v.physical(t)
			var retained *workflowplugin.Provider
			native := workflowplugin.Native(func(call context.Context, p *workflowplugin.Provider, _ workflowplugin.InvokeRequest) (workflowplugin.InvokeResult, error) {
				retained = p
				view, err := p.Get(call, b21SourceActor, ref("workflow:item/source-parent"), b21WriteMeta("host-read", "bead.get"))
				return workflowplugin.InvokeResult{Items: []workflowplugin.ItemView{view}}, err
			})
			config := pluginhost.Config{Natives: []sdk.Native{native}, CheckpointStore: v.cap, CheckpointLease: v.lease, SourcePlanes: []driver.SourcePlaneProviderCommitV1{prepared.Binding}}
			host, err := pluginhost.NewWithContext(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			params, _ := json.Marshal(map[string]any{"project": project})
			result, err := host.Invoke(ctx, "x.workflow.invoke", params)
			if err != nil {
				t.Fatal(err)
			}
			var response workflowplugin.InvokeResult
			if err := json.Unmarshal(result, &response); err != nil || len(response.Items) != 1 || response.Items[0].ID != "workflow:item/source-parent" {
				t.Fatalf("selected host callback read=%s %v", result, err)
			}
			if _, err := retained.Get(ctx, b21SourceActor, ref("workflow:item/source-parent"), b21WriteMeta("outside-host", "bead.get")); err == nil {
				t.Fatal("retained provider bypassed the host invocation fence")
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("host admission/read/refusal changed complete physical store")
			}
			if engine == "memory" {
				config.RequireDurableState = true
				if extra, err := pluginhost.NewWithContext(ctx, config); err == nil {
					_ = extra.Close()
					t.Fatal("process namespace satisfied a restart-durable requirement")
				}
				config.RequireDurableState = false
			}
			config.SourcePlanes = append(config.SourcePlanes, prepared.Binding)
			if extra, err := pluginhost.NewWithContext(ctx, config); err == nil {
				_ = extra.Close()
				t.Fatal("duplicate selected source namespace admitted")
			}
			if !reflect.DeepEqual(frozen, v.physical(t)) {
				t.Fatal("refused source admission changed complete physical store")
			}
		})
	}
}

// Snapshot every application table, preserving the complete column census,
// SQL NULL, actual SQL value types and full bytes. No lock/receipt/checkpoint
// table is removed to make a refusal pass.
func b21CompleteSQL(t *testing.T, db *sql.DB, engine string) map[string][]string {
	t.Helper()
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	if engine == "postgres" {
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema='plug_postgres' AND table_type='BASE TABLE' ORDER BY table_name"
	}
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name == "" || strings.ContainsAny(name, "\"\x00") {
			t.Fatal("unsafe application table")
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(tables) < 10 {
		t.Fatalf("incomplete application-table census: %v", tables)
	}
	result := map[string][]string{}
	for _, table := range tables {
		qualified := `"` + table + `"`
		if engine == "postgres" {
			qualified = `plug_postgres.` + qualified
		}
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+qualified)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		header, err := json.Marshal(columns)
		if err != nil {
			t.Fatal(err)
		}
		images := []string{}
		for rows.Next() {
			values, ptrs := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			cells := make([]map[string]any, len(columns))
			for i, value := range values {
				cells[i] = map[string]any{"column": columns[i], "type": fmt.Sprintf("%T", value), "value": value}
			}
			body, err := json.Marshal(cells)
			if err != nil {
				t.Fatal(err)
			}
			images = append(images, string(body))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(images)
		result[table] = append([]string{string(header)}, images...)
	}
	return result
}
