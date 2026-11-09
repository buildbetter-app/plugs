package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// This is a disposable admitted source fixture, not a native recording. The
// process/restart lifetime is supplied by the actual selected driver throughout.
func TestJointSourcePublicOrdinaryDeleteOrderingReplayAndConservation(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			var dsn string
			store, lease, reopen := openIdentityTarget(t, engine, func(value string) { dsn = value })
			p := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			remove := func(id string) model.Op {
				return model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref(id)}}
			}
			unlink := func(from, to, kind string) model.Op {
				return model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: ref(from), To: ref(to), LinkKind: kind}}
			}
			// Remove the independent ordinary blocking relation through the public
			// core writer before installing active-first source routing. The active
			// related edge to demo-8 remains, and must still fence its deletion.
			setup := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
			if _, err := setup.Apply(ctx, p.Admission.Actor, model.ChangeSet{Ops: []model.Op{unlink("demo-7", "demo-8", "blocks")}}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: fmt.Sprintf("%032d", 300)}); err != nil {
				t.Fatal("ordinary relation setup", err)
			}
			policy := &sourcePublicPolicy{}
			_, server := sourcePublicService(t, store, lease, p, report, policy)
			actor := p.Admission.Actor
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			if engine == "memory" && capability.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeProcess {
				t.Fatal("memory source lifetime was relabeled")
			}
			namespace := report.SourcePlanes.Providers[0].Provider.Namespace
			checkpoint := func() driver.Checkpoint {
				t.Helper()
				value, err := capability.LoadCheckpoint(ctx, namespace)
				if err != nil {
					t.Fatal(err)
				}
				return value
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
			refuse := func(actor model.PrincipalRef, key int, cs model.ChangeSet, code string) {
				t.Helper()
				before := savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", key))
				beforeCheckpoint := checkpoint()
				var physical map[string][]string
				if db != nil {
					physical = jointPhysicalSQL(t, db, engine)
				}
				sourcePublicCall(t, server, actor, "tx.apply", key, map[string]any{"changeSet": cs}, code)
				if !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", key))) || !reflect.DeepEqual(beforeCheckpoint, checkpoint()) {
					t.Fatal("refused delete changed core, history, source bytes, receipts or checkpoint")
				}
				if db != nil && !reflect.DeepEqual(physical, jointPhysicalSQL(t, db, engine)) {
					t.Fatal("refused delete changed complete physical SQL image")
				}
			}
			created := sourcePublicCall(t, server, actor, "bead.create", 301, map[string]any{"type": "issue", "fields": map[string]any{"title": "ordinary delete after source installation"}}, "")
			var createdCommit model.Commit
			decode(t, created.Result, &createdCommit)
			if len(createdCommit.Results) != 1 {
				t.Fatal("ordinary create result", createdCommit)
			}
			victim := createdCommit.Results[0].Ref
			ordinary := model.ChangeSet{Ops: []model.Op{remove(victim.ID)}}
			policy.deny = true
			refuse(actor, 302, ordinary, "forbidden")
			policy.deny = false
			beforeOrdinary := savedLabelImport(t, store, report.Archive, p, "")
			initial := checkpoint()
			state, err := pluginhost.NewSourceRuntimeState(ctx, capability, lease, report.SourcePlanes.Providers[0])
			if err != nil {
				t.Fatal(err)
			}
			provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
			if err != nil {
				t.Fatal(err)
			}
			origins, err := provider.SourceRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			deleted := sourcePublicCall(t, server, actor, "tx.apply", 302, map[string]any{"changeSet": ordinary}, "")
			var deletedCommit model.Commit
			decode(t, deleted.Result, &deletedCommit)
			head, _ := beforeOrdinary.Meta.Head.Uint64()
			if deleted.Meta.Replayed || deletedCommit.Revision != model.Natural64(head+1) || len(deletedCommit.Results) != 1 || deletedCommit.Results[0].Ref != victim || len(deletedCommit.Results[0].Record) != 0 || !reflect.DeepEqual(initial, checkpoint()) {
				t.Fatal("ordinary delete did not publish once without changing source checkpoint", deletedCommit)
			}
			sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": victim}, "not_found")
			ordinaryState := savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 302))
			retry := sourcePublicCall(t, server, actor, "tx.apply", 302, map[string]any{"changeSet": ordinary}, "")
			if !retry.Meta.Replayed || !bytes.Equal(retry.Result, deleted.Result) || !reflect.DeepEqual(ordinaryState, savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 302))) || !reflect.DeepEqual(initial, checkpoint()) {
				t.Fatal("ordinary delete replay republished a participant or changed its receipt")
			}
			// The raw source alias must resolve to the active item, never delete
			// its durable collision. Other active references require ordered unlink.
			refuse(actor, 303, model.ChangeSet{Ops: []model.Op{remove("demo-7")}}, "invalid_params")
			refuse(actor, 304, model.ChangeSet{Ops: []model.Op{remove("demo-8")}}, "ontology_guard_failed")
			mixedUnlink := unlink("demo-7", "demo-8", "related")
			refuse(actor, 305, model.ChangeSet{Ops: []model.Op{remove("demo-8"), mixedUnlink}}, "ontology_guard_failed")
			mixed := model.ChangeSet{Ops: []model.Op{mixedUnlink, remove("demo-8")}}
			refuse(model.PrincipalRef{Kind: "user", ID: "user:foreign-delete"}, 306, mixed, "forbidden")
			policy.deny = true
			refuse(actor, 307, mixed, "forbidden")
			policy.deny = false
			refuse(actor, 308, model.ChangeSet{Ops: []model.Op{mixedUnlink, remove("demo-8"), {Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("missing"), Fields: map[string]any{"title": "late refusal"}}}}}, "not_found")
			beforeMixed := savedLabelImport(t, store, report.Archive, p, "")
			beforeCheckpoint := checkpoint()
			original := encode(t, mixed)
			joined := sourcePublicCall(t, server, actor, "tx.apply", 309, map[string]any{"changeSet": mixed}, "")
			var commit model.Commit
			decode(t, joined.Result, &commit)
			head, _ = beforeMixed.Meta.Head.Uint64()
			if joined.Meta.Replayed || commit.Revision != model.Natural64(head+1) || len(commit.Results) != 2 || commit.Results[0].Ref != ref("workflow:item/restored") || commit.Results[1].Ref != ref("demo-8") || len(commit.Results[1].Record) != 0 || !bytes.Equal(original, encode(t, mixed)) {
				t.Fatal("mixed delete changed original request/order or did not publish once", commit)
			}
			var active workflowplugin.ItemView
			decode(t, commit.Results[0].Record, &active)
			for _, relation := range active.Links {
				if relation.To.ID == "demo-8" {
					t.Fatal("mixed delete retained its active reference")
				}
			}
			current, err := provider.Get(ctx, actor, ref("workflow:item/restored"), model.WriteMeta{Project: "demo"})
			if err != nil {
				t.Fatal(err)
			}
			var currentWire workflowplugin.ItemView
			decode(t, encode(t, current), &currentWire)
			if !reflect.DeepEqual(currentWire, active) {
				t.Fatal("mixed receipt is not the complete surviving active post-image")
			}
			sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": ref("demo-8")}, "not_found")
			afterCheckpoint := checkpoint()
			if afterCheckpoint.Generation != beforeCheckpoint.Generation+1 || bytes.Equal(afterCheckpoint.Payload, beforeCheckpoint.Payload) {
				t.Fatal("mixed delete failed to join its one source checkpoint")
			}
			laterOrigins, err := provider.SourceRecords(ctx)
			if err != nil || !reflect.DeepEqual(origins, laterOrigins) {
				t.Fatal("mixed deletion changed inert physical source origins", err)
			}
			joinedState := savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 309))
			if joinedState.WriteReceipt == nil || len(joinedState.WriteReceipt.Commit.Results) != 2 || len(joinedState.WriteReceipt.Commit.Results[0].Record) != 0 || !reflect.DeepEqual(beforeMixed.Blobs, joinedState.Blobs) || !reflect.DeepEqual(beforeMixed.ImportReceipt, joinedState.ImportReceipt) {
				t.Fatal("mixed delete changed source origins/import receipt or duplicated active payload in core receipt")
			}
			// A same-key rewritten request must fail. Replay is bound to the raw
			// alias request, rather than the resolved workflow target operation.
			refuse(actor, 309, model.ChangeSet{Ops: []model.Op{unlink("workflow:item/restored", "demo-8", "related"), remove("demo-8")}}, "idempotency_key_reused")
			replayed := sourcePublicCall(t, server, actor, "tx.apply", 309, map[string]any{"changeSet": mixed}, "")
			if !replayed.Meta.Replayed || !bytes.Equal(joined.Result, replayed.Result) || !reflect.DeepEqual(joinedState, savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 309))) || !reflect.DeepEqual(afterCheckpoint, checkpoint()) {
				t.Fatal("mixed delete replay changed result/core/checkpoint")
			}
			store, lease = reopen()
			capability, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found {
				t.Fatal("recover delete source runtime", err)
			}
			_, server = sourcePublicService(t, store, lease, bound, recovered, policy)
			replayed = sourcePublicCall(t, server, actor, "tx.apply", 309, map[string]any{"changeSet": mixed}, "")
			if !replayed.Meta.Replayed || !bytes.Equal(joined.Result, replayed.Result) || !reflect.DeepEqual(joinedState, savedLabelImport(t, store, report.Archive, p, fmt.Sprintf("%032d", 309))) || !reflect.DeepEqual(afterCheckpoint, checkpoint()) {
				t.Fatal("reopened mixed delete lost original result or republished state")
			}
			if policy.seen < 5 {
				t.Fatal("source delete bypassed ordinary writer authorization")
			}
		})
	}
}
