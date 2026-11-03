package postgresacceptance

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

type b15Page struct {
	Comments []struct {
		ID      uint64 `json:"id"`
		Comment struct {
			Body string `json:"body"`
		} `json:"comment"`
	} `json:"comments"`
	AsOf       map[string]any `json:"asOf"`
	NextCursor string         `json:"nextCursor"`
}

func readB15Page(t *testing.T, h *host, params map[string]any) b15Page {
	t.Helper()
	r := h.call(t, "bead.comments", params, "")
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	return decode[b15Page](t, r.Result)
}

func TestB15PublicCommentPagePostgresPinnedAcrossPhysicalReopen(t *testing.T) {
	const id = "pl-b15-page-anchor"
	const other = "pl-b15-page-other"
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs(id, other)
	h := startHost(t, openStore(t, dsn), sequence, trace, pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)))
	create(t, h, "b15-page-create", "page anchor", 1)
	create(t, h, "b15-page-other", "other anchor", 2)
	if empty := readB15Page(t, h, map[string]any{"id": ref(id)}); len(empty.Comments) != 0 || empty.NextCursor != "" {
		t.Fatalf("new PostgreSQL thread has comments: %+v", empty)
	}
	for i, body := range []string{"first", "second", "third"} {
		committed(t, h.call(t, "bead.comment", map[string]any{"id": ref(id), "body": body}, key("b15-page-comment-"+body)), uint64(3+i))
	}
	first := readB15Page(t, h, map[string]any{"id": ref(id), "limit": 1})
	if len(first.Comments) != 1 || first.Comments[0].ID != 1 || first.Comments[0].Comment.Body != "first" || first.NextCursor == "" {
		t.Fatalf("first PostgreSQL keyset page: %+v", first)
	}
	committed(t, h.call(t, "bead.comment", map[string]any{"id": ref(id), "body": "later"}, key("b15-page-comment-later")), 6)
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace, pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)))
	wrong := h.call(t, "bead.comments", map[string]any{"id": ref(other), "cursor": first.NextCursor, "limit": 1}, "")
	if wrong.Error == nil || wrong.Error.Data.Code != "cursor_invalid" {
		t.Fatalf("cross-thread cursor was accepted: %+v", wrong)
	}
	second := readB15Page(t, h, map[string]any{"id": ref(id), "cursor": first.NextCursor, "limit": 1})
	if len(second.Comments) != 1 || second.Comments[0].ID != 2 || second.Comments[0].Comment.Body != "second" || second.NextCursor == "" || !reflect.DeepEqual(second.AsOf, first.AsOf) {
		t.Fatalf("PostgreSQL pinned continuation changed: first=%+v second=%+v", first, second)
	}
	third := readB15Page(t, h, map[string]any{"id": ref(id), "cursor": second.NextCursor, "limit": 1})
	if len(third.Comments) != 1 || third.Comments[0].ID != 3 || third.Comments[0].Comment.Body != "third" || third.NextCursor != "" || !reflect.DeepEqual(third.AsOf, first.AsOf) {
		t.Fatalf("PostgreSQL pinned tail changed: first=%+v third=%+v", first, third)
	}
	live := readB15Page(t, h, map[string]any{"id": ref(id), "limit": 500})
	if len(live.Comments) != 4 || live.Comments[3].ID != 4 || live.Comments[3].Comment.Body != "later" || live.NextCursor != "" {
		t.Fatalf("fresh PostgreSQL page lost later comment: %+v", live)
	}
	wrongLimit := h.call(t, "bead.comments", map[string]any{"id": ref(id), "limit": 501}, "")
	if wrongLimit.Error == nil || wrongLimit.Error.Data.Code != "invalid_params" {
		t.Fatalf("overlarge PostgreSQL page was accepted: %+v", wrongLimit)
	}
	t.Log("B15 public PostgreSQL page: keyset, pinned continuation, physical reopen, cross-thread and limit refusal")
}
