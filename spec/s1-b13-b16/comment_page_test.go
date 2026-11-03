package s1b13b16

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

func pageEntry(t *testing.T, value map[string]any, wantID, wantBody string) {
	t.Helper()
	rows, ok := value["comments"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("expected one comment page item: %+v", value)
	}
	row, ok := rows[0].(map[string]any)
	if !ok || fmt.Sprint(row["id"]) != wantID {
		t.Fatalf("comment ordinal changed: %+v", row)
	}
	comment, ok := row["comment"].(map[string]any)
	if !ok || comment["body"] != wantBody || comment["principal"] == nil || comment["time"] == nil {
		t.Fatalf("comment body/actor/time changed: %+v", row)
	}
}

func commentPageRequest(id string, limit int, cursor string) map[string]any {
	selection := map[string]any{}
	if limit != 0 {
		selection["limit"] = limit
	}
	if cursor != "" {
		selection["cursor"] = cursor
	}
	return map[string]any{"id": ref(id), "commentPage": selection}
}

func selectedCommentPage(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	if value["bead"] == nil || value["attachments"] == nil {
		t.Fatalf("bead.get projection disappeared: %+v", value)
	}
	if _, ok := value["comments"]; ok {
		t.Fatalf("selected bead.get exposed the unbounded comment array: %+v", value)
	}
	page, ok := value["commentPage"].(map[string]any)
	if !ok {
		t.Fatalf("bead.get omitted the selected comment page: %+v", value)
	}
	return page
}

func TestB15PublicCommentKeysetPagingAndSQLiteReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, "b15-pages", []string{"b15-page-anchor", "b15-other-anchor"}, serviceOntology(t))
			f.call(t, context.Background(), "page-create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "anchor"}}, true)
			f.call(t, context.Background(), "other-create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "other"}}, true)
			if ordinary := result(t, f.call(t, context.Background(), "ordinary-get", "bead.get", map[string]any{"id": ref("b15-page-anchor")}, false)); ordinary["commentPage"] != nil {
				t.Fatalf("unselected bead.get changed its response: %+v", ordinary)
			}
			params := commentPageRequest("b15-page-anchor", 0, "")
			if empty := selectedCommentPage(t, result(t, f.call(t, context.Background(), "empty-page", "bead.get", params, false))); len(empty["comments"].([]any)) != 0 || empty["nextCursor"] != nil {
				t.Fatalf("empty thread page: %+v", empty)
			}
			for i, body := range []string{"  first  ", "second", "third"} {
				f.call(t, context.Background(), "page-comment-"+string(rune('a'+i)), "bead.comment", map[string]any{"id": ref("b15-page-anchor"), "body": body}, true)
			}
			ordinary := result(t, f.call(t, context.Background(), "ordinary-thread", "bead.get", map[string]any{"id": ref("b15-page-anchor")}, false))
			legacy, ok := ordinary["comments"].([]any)
			if !ok || len(legacy) != 3 || legacy[0].(map[string]any)["body"] != "  first  " || ordinary["commentPage"] != nil {
				t.Fatalf("unselected bead.get comment projection changed: %+v", ordinary)
			}
			first := selectedCommentPage(t, result(t, f.call(t, context.Background(), "first-page", "bead.get", commentPageRequest("b15-page-anchor", 1, ""), false)))
			pageEntry(t, first, "1", "  first  ")
			cursor, ok := first["nextCursor"].(string)
			if !ok || cursor == "" {
				t.Fatalf("first page did not advance: %+v", first)
			}
			pinned := first["asOf"]
			f.call(t, context.Background(), "page-comment-after", "bead.comment", map[string]any{"id": ref("b15-page-anchor"), "body": "later"}, true)
			if engine == "sqlite" {
				f.reopen(t)
			}
			refused(t, f.call(t, context.Background(), "cross-thread", "bead.get", commentPageRequest("b15-other-anchor", 1, cursor), false), "cursor_invalid")
			refused(t, f.call(t, context.Background(), "tampered", "bead.get", commentPageRequest("b15-page-anchor", 1, cursor+"x"), false), "cursor_invalid")
			refused(t, f.call(t, context.Background(), "over-limit", "bead.get", commentPageRequest("b15-page-anchor", 501, ""), false), "invalid_params")
			refused(t, f.call(t, context.Background(), "unknown", "bead.get", commentPageRequest("b15-absent", 0, ""), false), "not_found")
			for i, want := range []string{"second", "third"} {
				page := selectedCommentPage(t, result(t, f.call(t, context.Background(), "resume-"+string(rune('a'+i)), "bead.get", commentPageRequest("b15-page-anchor", 1, cursor), false)))
				pageEntry(t, page, string(rune('2'+i)), want)
				if !reflect.DeepEqual(page["asOf"], pinned) {
					t.Fatalf("cursor changed pinned thread revision: first=%v next=%v", pinned, page["asOf"])
				}
				cursor, _ = page["nextCursor"].(string)
				if i == 0 && cursor == "" || i == 1 && cursor != "" {
					t.Fatalf("unexpected pinned page tail: %+v", page)
				}
			}
			live := selectedCommentPage(t, result(t, f.call(t, context.Background(), "live-page", "bead.get", commentPageRequest("b15-page-anchor", 500, ""), false)))
			if got := len(live["comments"].([]any)); got != 4 || live["nextCursor"] != nil {
				t.Fatalf("new live thread did not include later comment: %+v", live)
			}
			last := live["comments"].([]any)[3].(map[string]any)["comment"].(map[string]any)["body"].(string)
			if last != "later" {
				t.Fatalf("live tail changed: %q", last)
			}
		})
	}
}

func serviceOntology(t *testing.T) *service.Ontology {
	t.Helper()
	return service.NewOntology(nil)
}
