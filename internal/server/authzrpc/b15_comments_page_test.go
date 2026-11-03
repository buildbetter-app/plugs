package authzrpc_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB15AuthenticatedCommentPageUsesCurrentBeadReadPermit(t *testing.T) {
	f := newFixture(t, nil)
	by := model.PrincipalRef{Kind: "user", ID: "user:operator"}
	stamp := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	doc := pipeline.Document{
		ID: beadID, Type: "issue", Project: "project",
		Fields: map[string]any{"title": "seed", "status": "todo", "priority": 0},
		Comments: []pipeline.Comment{
			{Body: "first", Principal: by, Time: stamp},
			{Body: "second", Principal: by, Time: stamp},
		},
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: model.RefByID(beadID, model.Natural64(0)), Kind: "bead", Op: model.OpPatch, Body: body}}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "commentPage": map[string]any{"limit": 1}}
	first := f.call(t, by, "bead.get", params, 21001)
	success(t, first)
	type commentPage struct {
		Comments []struct {
			ID      uint64 `json:"id"`
			Comment struct {
				Body string `json:"body"`
			} `json:"comment"`
		} `json:"comments"`
		NextCursor string `json:"nextCursor"`
	}
	var firstResult struct {
		CommentPage commentPage `json:"commentPage"`
	}
	if err := json.Unmarshal(first.Result, &firstResult); err != nil || len(firstResult.CommentPage.Comments) != 1 || firstResult.CommentPage.Comments[0].ID != 1 || firstResult.CommentPage.Comments[0].Comment.Body != "first" || firstResult.CommentPage.NextCursor == "" {
		t.Fatalf("authenticated first comment page: %+v, %v", firstResult, err)
	}
	denied := f.call(t, worker("user:operator"), "bead.get", map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "commentPage": map[string]any{}}, 21002)
	if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 {
		t.Fatalf("agent read bypassed fixed policy: %+v", denied)
	}
	other := model.PrincipalRef{Kind: "user", ID: "user:other"}
	wrongPrincipal := f.call(t, other, "bead.get", map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "commentPage": map[string]any{"limit": 1, "cursor": firstResult.CommentPage.NextCursor}}, 21003)
	if wrongPrincipal.Error == nil || wrongPrincipal.Error.Data.Code != "cursor_invalid" || len(wrongPrincipal.Result) != 0 {
		t.Fatalf("cursor crossed authenticated principals: %+v", wrongPrincipal)
	}
	second := f.call(t, by, "bead.get", map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "commentPage": map[string]any{"limit": 1, "cursor": firstResult.CommentPage.NextCursor}}, 21004)
	success(t, second)
	var secondResult struct {
		CommentPage commentPage `json:"commentPage"`
	}
	if err := json.Unmarshal(second.Result, &secondResult); err != nil || len(secondResult.CommentPage.Comments) != 1 || secondResult.CommentPage.Comments[0].ID != 2 || secondResult.CommentPage.Comments[0].Comment.Body != "second" || secondResult.CommentPage.NextCursor != "" {
		t.Fatalf("authenticated equal-time continuation: %+v, %v", secondResult, err)
	}
}
