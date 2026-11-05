package postgresacceptance

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// This test observes the real stored auxiliary arms, independently of public
// relation deduplication. An extra mirror or stale endpoint must fail the test.
type b25StoredItem struct {
	Parent       *model.Ref                    `json:"parent"`
	Links        []workflowplugin.RelationView `json:"links"`
	Inbound      []workflowplugin.RelationView `json:"inbound"`
	ChildCounter uint64                        `json:"childCounter"`
	Generation   uint64                        `json:"generation"`
}

func b25Items(t *testing.T, h *b26HTTP) map[string]b25StoredItem {
	t.Helper()
	result := map[string]b25StoredItem{}
	for id, raw := range workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Items {
		result[id] = decode[b25StoredItem](t, raw)
	}
	return result
}

func b25Edges(t *testing.T, edges []workflowplugin.RelationView) []string {
	t.Helper()
	result := []string{}
	for _, edge := range edges {
		data, err := json.Marshal(edge)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, string(data))
	}
	sort.Strings(result)
	return result
}

func b25Edge(from, to, kind string) workflowplugin.RelationView {
	return workflowplugin.RelationView{From: from, To: ref(to), Kind: kind}
}

func b25AssertEdges(t *testing.T, actual, expected []workflowplugin.RelationView) {
	t.Helper()
	if a, e := b25Edges(t, actual), b25Edges(t, expected); !reflect.DeepEqual(a, e) {
		t.Fatalf("complete relation multiset differs: got=%v want=%v", a, e)
	}
}

func b25Unlink(from, to, kind string) model.Op {
	return model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: ref(from), To: ref(to), LinkKind: kind}}
}

func b25Delete(id string) model.Op {
	return model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref(id)}}
}

func b25Link(t *testing.T, h *b26HTTP, from, to, kind, label string) {
	t.Helper()
	b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(from), "to": ref(to), "kind": kind}, label))
}

func b25AuxFault(t *testing.T, db *sql.DB, engine string) func() {
	t.Helper()
	install := `CREATE TRIGGER b25_aux_failure BEFORE INSERT ON versions BEGIN SELECT RAISE(ABORT,'b25 auxiliary version failure'); END`
	remove := `DROP TRIGGER b25_aux_failure`
	if engine == "postgres" {
		install = `CREATE FUNCTION plug_postgres.b25_aux_failure() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'b25 auxiliary version failure'; END$$; CREATE TRIGGER b25_aux_failure BEFORE INSERT ON plug_postgres.versions FOR EACH ROW EXECUTE FUNCTION plug_postgres.b25_aux_failure()`
		remove = `DROP TRIGGER b25_aux_failure ON plug_postgres.versions; DROP FUNCTION plug_postgres.b25_aux_failure()`
	}
	if _, err := db.ExecContext(context.Background(), install); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if _, err := db.ExecContext(context.Background(), remove); err != nil {
			t.Fatal(err)
		}
	}
}

func TestB25PublicMixedDeleteAndDemotionSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b25-mixed.db")
	open := func() driver.Store {
		store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b25MixedDeleteAndDemotion(t, open, db, "sqlite")
}

func TestB25PublicMixedDeleteAndDemotionPostgreSQL(t *testing.T) {
	dsn := freshDatabase(t)
	open := func() driver.Store { return openStore(t, dsn) }
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b25MixedDeleteAndDemotion(t, open, db, "postgres")
}

func b25MixedDeleteAndDemotion(t *testing.T, open func() driver.Store, db *sql.DB, engine string) {
	h := b26Host(t, open())
	source, target, dependent := h.create(t, "b25 source"), h.create(t, "b25 target"), h.create(t, "b25 dependent")
	parent := h.create(t, "b25 parent")
	parentRef := ref(parent)
	b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(source), "fields": map[string]any{"labels": []string{"one", "two"}}}, "b25-labels"))
	b26OK(t, h.call(t, "bead.comment", map[string]any{"id": ref(source), "body": "b25 retained comment"}, "b25-comment"))
	b26OK(t, h.call(t, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref(source), Parent: &parentRef}},
		{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref(source), Key: "user.b25", Value: json.RawMessage(`{"retained":[true,null,42]}`)}},
	}}}, "b25-auxiliary-setup"))
	invoked := decode[workflowplugin.InvokeResult](t, b26OK(t, h.call(t, "x.workflow.invoke", map[string]any{
		"template": "b25", "project": project, "mode": "ephemeral", "checklist": []map[string]any{
			{"label": "outgoing", "title": "outgoing", "type": "issue"},
			{"label": "incoming", "title": "incoming", "type": "issue"},
			{"label": "child", "title": "child", "type": "issue", "parent": ref(source)},
		},
	}, "b25-invoke")))
	outgoing, incoming, child := invoked.Items[0].ID, invoked.Items[1].ID, invoked.Items[2].ID
	b25Link(t, h, source, target, "relates-to", "b25-core-outbound")
	b25Link(t, h, dependent, source, "blocks", "b25-core-inbound")
	b25Link(t, h, source, outgoing, "relates-to", "b25-mixed-outbound")
	b25Link(t, h, incoming, source, "relates-to", "b25-mixed-inbound")
	beforePublic := h.get(t, source)
	before := b26CompleteSQL(t, db, engine)
	params := map[string]any{"source": ref(source), "template": "b25-demoted", "label": "moved"}
	removeFault := b25AuxFault(t, db, engine)
	failed := h.call(t, "x.workflow.run.demote", params, "b25-demotion")
	removeFault()
	if failed.Error == nil || len(failed.Result) != 0 || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
		t.Fatal("failed demotion did not conserve every SQL row/column, checkpoint, history, head and receipt")
	}
	first := b26OK(t, h.call(t, "x.workflow.run.demote", params, "b25-demotion"))
	moved := decode[workflowplugin.DemoteResult](t, first)
	id := moved.Item.ID
	items := b25Items(t, h)
	b25AssertEdges(t, items[id].Links, []workflowplugin.RelationView{b25Edge(id, target, "relates-to"), b25Edge(id, outgoing, "relates-to")})
	b25AssertEdges(t, items[id].Inbound, []workflowplugin.RelationView{b25Edge(dependent, id, "blocks"), b25Edge(incoming, id, "relates-to")})
	b25AssertEdges(t, items[outgoing].Inbound, []workflowplugin.RelationView{b25Edge(id, outgoing, "relates-to")})
	b25AssertEdges(t, items[incoming].Links, []workflowplugin.RelationView{b25Edge(incoming, id, "relates-to")})
	if items[child].Parent == nil || items[child].Parent.ID != id || items[id].ChildCounter != 1 || items[id].Parent == nil || items[id].Parent.ID != parent {
		t.Fatalf("demotion lost typed parent or child counter: moved=%+v child=%+v", items[id], items[child])
	}
	for activeID, item := range items {
		for _, arm := range [][]workflowplugin.RelationView{item.Links, item.Inbound} {
			for _, edge := range arm {
				if edge.From == source || edge.To.ID == source {
					t.Fatalf("active %s retained deleted source reference: %+v", activeID, edge)
				}
			}
		}
	}
	afterPublic := h.get(t, id)
	for _, field := range []string{"comments"} {
		if !reflect.DeepEqual(beforePublic[field], afterPublic[field]) {
			t.Fatalf("demotion changed %s", field)
		}
	}
	for _, field := range []string{"labels", "metadata"} {
		if !reflect.DeepEqual(beforePublic["bead"].(map[string]any)[field], afterPublic["bead"].(map[string]any)[field]) {
			t.Fatalf("demotion changed %s", field)
		}
	}
	b26NoVersion(t, h.store, id)
	if absent := h.call(t, "bead.get", map[string]any{"id": ref(source)}, ""); absent.Error == nil || absent.Error.Data.Code != "not_found" {
		t.Fatalf("demotion retained its ordinary source: %+v", absent)
	}
	committed := b26CompleteSQL(t, db, engine)
	replayed := h.call(t, "x.workflow.run.demote", params, "b25-demotion")
	if replayed.Error != nil || !reflect.DeepEqual(decode[any](t, first), decode[any](t, replayed.Result)) || !reflect.DeepEqual(committed, b26CompleteSQL(t, db, engine)) {
		t.Fatal("demotion replay moved auxiliaries a second time")
	}

	// The same public transaction removes both workflow directions, both core
	// directions and an owned body. There is no separate cleanup publication.
	victim, coreIn := h.create(t, "b25 delete victim"), h.create(t, "b25 delete dependent")
	b26OK(t, h.call(t, "bead.comment", map[string]any{"id": ref(victim), "body": "owned delete comment"}, "b25-delete-comment"))
	b25Link(t, h, victim, target, "relates-to", "b25-delete-core-out")
	b25Link(t, h, coreIn, victim, "relates-to", "b25-delete-core-in")
	b25Link(t, h, victim, outgoing, "relates-to", "b25-delete-mixed-out")
	b25Link(t, h, incoming, victim, "relates-to", "b25-delete-mixed-in")
	coreOps := []model.Op{b25Unlink(victim, target, "relates-to"), b25Unlink(coreIn, victim, "relates-to")}
	bad := append(append([]model.Op{}, coreOps...), b25Delete(victim))
	b26SQLRefusal(t, h, db, engine, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: bad}}, "b25-incomplete-delete", "ontology_guard_failed")
	ops := append(append([]model.Op{}, coreOps...), b25Unlink(victim, outgoing, "relates-to"), b25Unlink(incoming, victim, "relates-to"), b25Delete(victim))
	deleteParams := map[string]any{"changeSet": model.ChangeSet{Ops: ops}}
	before = b26CompleteSQL(t, db, engine)
	beforeMeta, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeItems := b25Items(t, h)
	removeFault = b25AuxFault(t, db, engine)
	failed = h.call(t, "tx.apply", deleteParams, "b25-delete")
	removeFault()
	if failed.Error == nil || len(failed.Result) != 0 || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
		t.Fatal("failed mixed delete changed complete physical SQL state")
	}
	deletedReply := h.call(t, "tx.apply", deleteParams, "b25-delete")
	deleted := decode[model.Commit](t, b26OK(t, deletedReply))
	n, _ := beforeMeta.Head.Uint64()
	if deletedReply.replayed() || deleted.Revision != model.Natural64(n+1) || len(deleted.Results) != len(ops) || len(deleted.Results[4].Record) != 0 {
		t.Fatalf("mixed delete did not publish all original positions in one revision: %+v", deleted)
	}
	for position, active := range map[int]string{2: outgoing, 3: incoming} {
		b25AssertFullWorkflowResult(t, h, deleted.Results[position], active)
	}
	items = b25Items(t, h)
	if items[outgoing].Generation != beforeItems[outgoing].Generation+1 || items[incoming].Generation != beforeItems[incoming].Generation+1 {
		t.Fatal("mixed unlinks changed endpoint generation other than exactly once")
	}
	b25AssertEdges(t, items[outgoing].Inbound, []workflowplugin.RelationView{b25Edge(id, outgoing, "relates-to")})
	b25AssertEdges(t, items[incoming].Links, []workflowplugin.RelationView{b25Edge(incoming, id, "relates-to")})
	for _, item := range items {
		for _, arm := range [][]workflowplugin.RelationView{item.Links, item.Inbound} {
			for _, edge := range arm {
				if edge.From == victim || edge.To.ID == victim {
					t.Fatal("mixed delete retained a workflow auxiliary reference")
				}
			}
		}
	}
	b25AssertEdges(t, items[id].Links, beforeItems[id].Links)
	b25AssertEdges(t, items[id].Inbound, beforeItems[id].Inbound)
	checkpoint := workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint
	h.server.Close()
	if err := h.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}
	h = b26Host(t, open())
	// Acquiring a new lease intentionally changes its own physical rows. The
	// checkpoint and complete active census must persist byte-for-byte; every
	// table is then retained in the new replay-conservation baseline.
	if !reflect.DeepEqual(items, b25Items(t, h)) || !reflect.DeepEqual(checkpoint, workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint) {
		t.Fatal("close/reopen changed stored mixed relations or namespace")
	}
	committed = b26CompleteSQL(t, db, engine)
	replay := h.call(t, "tx.apply", deleteParams, "b25-delete")
	if replay.Error != nil || !replay.replayed() || !reflect.DeepEqual(decode[any](t, deletedReply.Result), decode[any](t, replay.Result)) || !reflect.DeepEqual(committed, b26CompleteSQL(t, db, engine)) {
		t.Fatal("mixed delete replay changed current relations, history or receipts")
	}
	changedOps := append([]model.Op{}, ops...)
	changedOps[2] = b25Unlink(victim, outgoing, "blocks")
	b26SQLRefusal(t, h, db, engine, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: changedOps}}, "b25-delete", "idempotency_key_reused")
	if gone := h.call(t, "bead.get", map[string]any{"id": ref(victim)}, ""); gone.Error == nil || gone.Error.Data.Code != "not_found" {
		t.Fatal("deleted owner remained publicly readable after reopen")
	}
	b25DeleteResultControls(t, h, db, engine)
	t.Log("B25 exact physical arm multiplicities, inbound rebinding, owned body/counter preservation, one-revision mixed explicit unlink/delete, actual SQL auxiliary fault conservation and original receipt replay after reopen")
}

func TestB25MixedMovesMemoryKeepsAdoptedCapabilityRefusal(t *testing.T) {
	store := memory.New()
	h := b26Host(t, store)
	source := h.create(t, "b25 memory source")
	before, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	public := h.get(t, source)
	denied := h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(source), "template": "b25", "label": "source"}, "b25-memory-demote")
	after, err := store.Meta(context.Background())
	if denied.Error == nil || denied.Error.Data.Code != "plugin_durable_state_unavailable" || err != nil || before != after || !reflect.DeepEqual(public, h.get(t, source)) {
		t.Fatal("memory demotion did not preserve its adopted capability refusal and source")
	}
	if checkpoint := workflowCheckpoint(t, store); checkpoint.Checkpoint.Generation != 0 {
		t.Fatal("memory refusal invented a persistent workflow namespace")
	}
}
