package postgresacceptance

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

type publicComment struct {
	Body      string             `json:"body"`
	Principal model.PrincipalRef `json:"principal"`
	Time      string             `json:"time"`
}

type commentThread struct {
	Bead     bead            `json:"bead"`
	Comments []publicComment `json:"comments"`
}

func readCommentThread(t *testing.T, h *host, id string, asOf *model.Revision) commentThread {
	t.Helper()
	params := map[string]any{"id": ref(id)}
	if asOf != nil {
		params["asOf"] = map[string]any{"revision": *asOf}
	}
	r := h.call(t, "bead.get", params, "")
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	return decode[commentThread](t, r.Result)
}

func TestB15PublicCommentsPostgresHistoryRefusalAndReopen(t *testing.T) {
	const id = "pl-b15-anchor"
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs(id)
	audit := pipeline.WithOntologyAudit(service.NewOntologyAudit(nil))
	h := startHost(t, openStore(t, dsn), sequence, trace, audit)
	create(t, h, "b15-create", "comment anchor", 1)
	original := readCommentThread(t, h, id, nil)
	if len(original.Comments) != 0 {
		t.Fatalf("new thread is not empty: %+v", original)
	}

	body := "  **literal**\n\nUnicode: café 東京\n"
	firstParams := map[string]any{"id": ref(id), "body": body}
	first := h.call(t, "bead.comment", firstParams, key("b15-comment-one"))
	committed(t, first, 2)
	replay(t, h.call(t, "bead.comment", firstParams, key("b15-comment-one")), first.Result)
	second := h.call(t, "bead.comment", map[string]any{"id": ref(id), "body": "second comment"}, key("b15-comment-two"))
	committed(t, second, 3)
	thread := readCommentThread(t, h, id, nil)
	if len(thread.Comments) != 2 || thread.Comments[0].Body != body || thread.Comments[1].Body != "second comment" {
		t.Fatalf("public comment body/order changed: %+v", thread.Comments)
	}
	if !reflect.DeepEqual(thread.Bead.Fields, original.Bead.Fields) {
		t.Fatalf("comments changed anchor fields: before=%v after=%v", original.Bead.Fields, thread.Bead.Fields)
	}
	for i, comment := range thread.Comments {
		wantTime := clock.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		if !reflect.DeepEqual(comment.Principal, actor) || comment.Time != wantTime {
			t.Fatalf("comment %d actor/time=%+v want actor=%+v time=%s", i, comment, actor, wantTime)
		}
	}
	atCreate, atFirst := model.Natural64(1), model.Natural64(2)
	if got := readCommentThread(t, h, id, &atCreate); len(got.Comments) != 0 {
		t.Fatalf("historical create leaked later comments: %+v", got)
	}
	if got := readCommentThread(t, h, id, &atFirst); len(got.Comments) != 1 || !reflect.DeepEqual(got.Comments[0], thread.Comments[0]) {
		t.Fatalf("historical first comment changed: %+v", got)
	}

	state := graph(t, h.store)
	ledger(t, state, 3, map[string]int{id: 3})
	for i, entry := range state.Changesets[1:] {
		if len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpComment || !slices.Contains(entry.Changes[0].ChangedPaths, "/comments") || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadCommented {
			t.Fatalf("comment %d split or changed its durable history/event: %+v", i, entry)
		}
	}
	for _, tc := range []struct {
		name, id, body, code string
	}{
		{"empty", id, "", "invalid_params"},
		{"whitespace", id, " \t\n", "invalid_params"},
		{"missing", "pl-b15-missing", "nonblank", "not_found"},
		{"prefix", "pl-b15", "nonblank", "not_found"},
	} {
		requestKey := key("b15-refuse-" + tc.name)
		refused(t, h.call(t, "bead.comment", map[string]any{"id": ref(tc.id), "body": tc.body}, requestKey), tc.code)
		if receipt(t, h.store, "bead.comment", requestKey, agent) != nil {
			t.Fatalf("%s refusal persisted a success receipt", tc.name)
		}
		unchanged(t, state, graph(t, h.store))
	}

	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace, audit)
	if got := readCommentThread(t, h, id, nil); !reflect.DeepEqual(got, thread) {
		t.Fatalf("physical PostgreSQL reopen changed public thread: before=%+v after=%+v", thread, got)
	}
	replay(t, h.call(t, "bead.comment", firstParams, key("b15-comment-one")), first.Result)
	unchanged(t, state, graph(t, h.store))
	t.Log("B15 public PostgreSQL: exact body/actor, fixed-clock order, historical snapshots, one event per comment, no-write refusals, receipt replay and physical reopen")
}
