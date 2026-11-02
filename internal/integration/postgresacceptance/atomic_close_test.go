package postgresacceptance

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"testing"
)

func TestB21AtomicCloseRealPostgresHTTPAndReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-close-a", "pl-close-b")
	h := startHost(t, openStore(t, dsn), sequence, trace)
	create(t, h, "close-seed", "seed", 1)
	params := map[string]any{"changeSet": map[string]any{"ops": []any{
		map[string]any{"kind": "create", "key": "new", "type": "issue", "fields": map[string]any{"title": "new"}},
		map[string]any{"kind": "patch", "target": model.RefByKey("new"), "fields": map[string]any{"title": "prefix"}},
		map[string]any{"kind": "close", "target": model.RefByKey("new"), "reason": "new reason"},
		map[string]any{"kind": "close", "target": ref("pl-close-a"), "reason": "existing reason", "expect": []model.Precondition{model.PreconditionByRevision(ref("pl-close-a"), model.Natural64(1))}},
	}}}
	first := h.call(t, "tx.apply", params, key("pg-close-batch"))
	c := committed(t, first, 2)
	if len(c.Results) != 4 || c.Results[2].Ref.ID != "pl-close-b" || c.Results[3].Ref.ID != "pl-close-a" {
		t.Fatalf("close result order: %+v", c)
	}
	for _, id := range []string{"pl-close-a", "pl-close-b"} {
		if got := get(t, h, id, nil); got.Fields["status"] != "done" {
			t.Fatalf("not closed: %+v", got)
		}
	}
	after := graph(t, h.store)
	ledger(t, after, 2, map[string]int{"pl-close-a": 2, "pl-close-b": 1})
	if len(after.Changesets[1].Changes) != 2 {
		t.Fatal("close batch split its durable history")
	}
	replay(t, h.call(t, "tx.apply", params, key("pg-close-batch")), first.Result)
	unchanged(t, after, graph(t, h.store))
	refusedParams := map[string]any{"changeSet": map[string]any{"ops": []any{
		map[string]any{"kind": "patch", "target": ref("pl-close-a"), "fields": map[string]any{"title": "must rollback"}},
		map[string]any{"kind": "close", "target": ref("pl-close-a"), "reason": "must not append", "expect": []model.Precondition{model.PreconditionByRevision(ref("pl-close-a"), model.Natural64(1))}},
	}}}
	stale := h.call(t, "tx.apply", refusedParams, key("pg-close-stale"))
	refused(t, stale, "precondition_failed")
	if stale.Error.Data.Details["reason"] != "revision" || receipt(t, h.store, "tx.apply", key("pg-close-stale"), agent) != nil {
		t.Fatalf("private close guard/receipt: %+v", stale)
	}
	unchanged(t, after, graph(t, h.store))
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace)
	replay(t, h.call(t, "tx.apply", params, key("pg-close-batch")), first.Result)
	unchanged(t, after, graph(t, h.store))
	t.Log("B21 public PostgreSQL close batch: declaration order, backward alias, reasons, one revision/history, private revision refusal, no receipt on refusal and full handle reopen replay")
}
