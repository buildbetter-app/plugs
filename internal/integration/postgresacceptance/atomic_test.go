package postgresacceptance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func meta(method, label string) model.WriteMeta {
	return model.WriteMeta{Method: method, Project: project, AgentLabel: agent, IdempotencyKey: key(label)}
}

func patch(id string, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(id), Fields: fields}}
}

func direct(t *testing.T, h *host, cs model.ChangeSet, method, label string, revision uint64) model.Commit {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := h.writer.ApplyWithOutcome(ctx, actor, cs, meta(method, label))
	if err != nil || out.Replayed || out.Commit.Revision.String() != model.Natural64(revision).String() {
		t.Fatalf("direct real Writer %s: %+v %v", method, out, err)
	}
	return out.Commit
}

func domainRefusal(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Fatalf("expected direct %s refusal: %v", code, err)
	}
}

func TestB08CloseGuardRefusalAndReopenAreAtomic(t *testing.T) {
	h := startHost(t, openStore(t, freshDatabase(t)), newIDs("pl-a", "pl-b"), newTrace(t))
	create(t, h, "guard-blocker", "blocker", 1)
	create(t, h, "guard-dependent", "dependent", 2)
	// Graph setup and reopen use the one actual Writer because these methods
	// are not bound by the admitted RPC service. No synthetic handler is added.
	direct(t, h, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: ref("pl-b"), To: ref("pl-a"), LinkKind: "blocks"}}}}, "tx.apply", "guard-edge", 3)
	filter := map[string]any{"resources": []string{"pl-b"}}
	page(t, h.call(t, "bead.ready", map[string]any{"filter": filter}, ""), 3)
	before := graph(t, h.store)
	refused(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b"), "reason": "must not be appended"}, key("guard-refused")), "ontology_guard_failed")
	unchanged(t, before, graph(t, h.store))
	if receipt(t, h.store, "bead.close", key("guard-refused"), agent) != nil {
		t.Fatal("guard refusal published a receipt")
	}
	committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-a")}, key("guard-close-blocker")), 4)
	page(t, h.call(t, "bead.ready", map[string]any{"filter": filter}, ""), 4, "pl-b")
	const reason = "  resolved with evidence\nsecond line  "
	closed := committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b"), "reason": reason}, key("guard-close-dependent")), 5)
	if len(closed.Results) != 2 {
		t.Fatalf("close/reason did not retain both operation results: %+v", closed)
	}
	doc, err := pipeline.DecodeDocument(closed.Results[0].Record)
	if err != nil || doc.Fields["status"] != "done" || len(doc.Comments) != 1 || doc.Comments[0].Body != reason || !reflect.DeepEqual(doc.Comments[0].Principal, actor) || !doc.Comments[0].Time.Equal(clock) {
		t.Fatalf("close/reason was not one exact final image: %+v %v", doc, err)
	}
	before = graph(t, h.store)
	ledger(t, before, 5, map[string]int{"pl-a": 2, "pl-b": 3})
	if len(before.Changesets[4].Changes) != 1 {
		t.Fatal("close plus reason minted multiple changed-resource entries")
	}
	committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b")}, key("guard-close-noop")), 5)
	unchanged(t, before, graph(t, h.store))
	refused(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b"), "expect": []model.Precondition{model.PreconditionByRevision(ref("pl-b"), model.Natural64(3))}}, key("guard-close-stale")), "precondition_failed")
	unchanged(t, before, graph(t, h.store))
	direct(t, h, model.ChangeSet{Ops: []model.Op{patch("pl-a", map[string]any{})}}, "bead.reopen", "guard-reopen-blocker", 6)
	direct(t, h, model.ChangeSet{Ops: []model.Op{patch("pl-b", map[string]any{})}}, "bead.reopen", "guard-reopen-dependent", 7)
	if got := get(t, h, "pl-b", nil); got.Status != "todo" || got.Revision.String() != "7" {
		t.Fatalf("direct reopen did not persist its actual slot: %+v", got)
	}
	page(t, h.call(t, "bead.ready", map[string]any{"filter": filter}, ""), 7)
	before = graph(t, h.store)
	refused(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b")}, key("guard-refused-again")), "ontology_guard_failed")
	unchanged(t, before, graph(t, h.store))
	ledger(t, before, 7, map[string]int{"pl-a": 3, "pl-b": 4})
	t.Log("B08 bounded: close with reason refuses under a live blocker without comment/version/log/receipt; close and fresh-key settlement are atomic; stale CAS still refuses; actual Writer reopen reblocks")
}

func TestB21BatchRefusalAndOrderedBackwardReferencesAreAtomic(t *testing.T) {
	h := startHost(t, openStore(t, freshDatabase(t)), newIDs("pl-a", "pl-b", "pl-c"), newTrace(t))
	create(t, h, "batch-anchor", "anchor", 1)
	before := graph(t, h.store, "pl-b", "pl-c", "pl-missing")
	failed := model.ChangeSet{Ops: []model.Op{
		patch("pl-a", map[string]any{"title": "must roll back"}),
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new", Type: "issue", Fields: map[string]any{"title": "must not exist"}}},
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref("pl-missing"), Body: "late refusal"}},
	}}
	input, err := json.Marshal(failed)
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.writer.ApplyWithOutcome(context.Background(), actor, failed, meta("tx.apply", "batch-refused"))
	domainRefusal(t, err, "not_found")
	if !out.Commit.Revision.IsZero() || len(out.Commit.Results) != 0 || out.Replayed {
		t.Fatalf("refused atomic batch returned partial results: %+v", out)
	}
	if after, err := json.Marshal(failed); err != nil || string(after) != string(input) {
		t.Fatal("pipeline mutated caller-owned refused request")
	}
	unchanged(t, before, graph(t, h.store, "pl-b", "pl-c", "pl-missing"))
	if receipt(t, h.store, "tx.apply", key("batch-refused"), agent) != nil {
		t.Fatal("refused batch published a receipt")
	}
	created := model.RefByKey("new")
	cs := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new", Type: "issue", Fields: map[string]any{"title": "first"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: created, Fields: map[string]any{"title": "last"}}},
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: created, Body: "batch comment"}},
		patch("pl-a", map[string]any{"description": "same transaction"}),
		{Kind: model.OpLink, Link: &model.LinkOp{From: created, To: ref("pl-a"), LinkKind: "relates-to"}},
	}}
	input, err = json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	commit := direct(t, h, cs, "tx.apply", "batch-success", 2)
	if len(commit.Results) != 5 {
		t.Fatalf("ordered batch lost per-operation results: %+v", commit)
	}
	for i, name := range []string{"pl-c", "pl-c", "pl-c", "pl-a", "pl-c"} {
		if commit.Results[i].Ref.ID != name || commit.Results[i].Revision.String() != "2" {
			t.Fatalf("batch result %d not resolved in declaration order: %+v", i, commit.Results[i])
		}
	}
	state := graph(t, h.store, "pl-b", "pl-c", "pl-missing")
	ledger(t, state, 2, map[string]int{"pl-a": 2, "pl-b": 0, "pl-c": 1, "pl-missing": 0})
	if len(state.Records) != 2 || len(state.Changesets[1].Changes) != 2 {
		t.Fatal("batch did not publish exactly two final record versions in one changeset")
	}
	doc, err := pipeline.DecodeDocument(commit.Results[0].Record)
	if err != nil || doc.ID != "pl-c" || doc.Fields["title"] != "last" || len(doc.Comments) != 1 || doc.Comments[0].Body != "batch comment" || len(doc.Links) != 1 || doc.Links[0].To.ID != "pl-a" {
		t.Fatalf("ordered batch final image: %+v %v", doc, err)
	}
	if after, err := json.Marshal(cs); err != nil || string(after) != string(input) {
		t.Fatal("pipeline mutated caller-owned successful request")
	}
	replayed, err := h.writer.ApplyWithOutcome(context.Background(), actor, cs, meta("tx.apply", "batch-success"))
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, commit) {
		t.Fatalf("direct batch durable replay: %+v %v", replayed, err)
	}
	unchanged(t, state, graph(t, h.store, "pl-b", "pl-c", "pl-missing"))
	t.Log("B21 bounded direct Writer: final-item refusal rolls back prior patch/create; backward alias patch/comment/link plus another record land one revision and one version per changed record; original request and stored replay stay identical")
}

func TestB18B22LateSQLRefusalLeavesNoPartialStateAfterReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-a")
	post := &warningPost{}
	options := pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "rollback-observer", Subscriber: post})
	h := startHost(t, openStore(t, dsn), sequence, trace, options)
	create(t, h, "sql-create", "original", 1)
	independent := openStore(t, dsn)
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	// nextval is deliberately nontransactional: its value proves this precise
	// late SQL gate ran, after record/version/predicate writes were staged.
	const install = `
CREATE SEQUENCE public.v3_accept_failure_hit;
CREATE FUNCTION public.v3_accept_reject_log() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM plug_postgres.records WHERE revision=NEW.revision)
 OR NOT EXISTS (SELECT 1 FROM plug_postgres.versions WHERE revision=NEW.revision)
 OR NOT EXISTS (SELECT 1 FROM plug_postgres.states WHERE revision=NEW.revision)
 THEN RAISE EXCEPTION 'acceptance publication did not reach staged families'; END IF;
 PERFORM nextval('public.v3_accept_failure_hit');
 RAISE EXCEPTION 'synthetic acceptance late publication refusal';
END$$;
CREATE TRIGGER v3_accept_reject_log BEFORE INSERT ON plug_postgres.changesets
FOR EACH ROW EXECUTE FUNCTION public.v3_accept_reject_log();`
	if _, err = admin.ExecContext(context.Background(), install); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"id": ref("pl-a"), "fields": map[string]any{"title": "after retry"}}
	before := graph(t, independent)
	refused(t, h.call(t, "bead.update", params, key("sql-retry")), "internal_error")
	var hit int64
	var called bool
	if err = admin.QueryRowContext(context.Background(), `SELECT last_value,is_called FROM public.v3_accept_failure_hit`).Scan(&hit, &called); err != nil || hit != 1 || !called {
		t.Fatalf("real late SQL fault was not reached exactly once: hit=%d called=%v error=%v", hit, called, err)
	}
	unchanged(t, before, graph(t, independent))
	if post.calls.Load() != 1 || receipt(t, independent, "bead.update", key("sql-retry"), agent) != nil {
		t.Fatal("SQL rollback ran post subscribers or published a receipt")
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	if err := independent.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, dsn)
	unchanged(t, before, graph(t, reopened))
	if receipt(t, reopened, "bead.update", key("sql-retry"), agent) != nil {
		t.Fatal("all-pool reopen exposed a refused response receipt")
	}
	if _, err = admin.ExecContext(context.Background(), `DROP TRIGGER v3_accept_reject_log ON plug_postgres.changesets; DROP FUNCTION public.v3_accept_reject_log(); DROP SEQUENCE public.v3_accept_failure_hit`); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, reopened, sequence, trace, options)
	retried := h.call(t, "bead.update", params, key("sql-retry"))
	committed(t, retried, 2)
	after := graph(t, reopened)
	ledger(t, after, 2, map[string]int{"pl-a": 2})
	if post.calls.Load() != 2 || get(t, h, "pl-a", nil).Fields["title"] != "after retry" {
		t.Fatal("same-key retry did not publish the one new state and post phase")
	}
	replay(t, h.call(t, "bead.update", params, key("sql-retry")), retried.Result)
	unchanged(t, after, graph(t, reopened))
	if post.calls.Load() != 2 {
		t.Fatal("same-key SQL retry replay ran post again")
	}
	t.Log("B18/B22/B28 bounded: trigger proves records/versions/predicates staged before SQL refusal; independent and reopened pools see no partial family, head or receipt; same-key retry consumes exactly revision 2 and runs post once")
}
