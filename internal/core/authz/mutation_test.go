package authz

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func bead(body map[string]any) model.Record {
	return model.Record{Kind: "bead", Ref: ref("b"), Revision: model.Natural64(1), Body: body}
}
func TestFixedSourceWriteGuard(t *testing.T) {
	c := request().Caller
	c.Principal = model.PrincipalRef{Kind: "agent", ID: "a", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u"}}
	before := bead(map[string]any{"project": "web/api", "parent": map[string]any{"id": "parent"}, "title": "before", "type": "bug", "owner": "u", "assignee": "a", "labels": []any{"x"}, "restricted": true})
	copyBody := func() map[string]any {
		b := map[string]any{}
		for k, v := range before.Body {
			b[k] = v
		}
		return b
	}
	for _, method := range []string{"bead.update", "bead.claim", "bead.claimNext"} {
		after := bead(copyBody())
		after.Body["title"] = "after"
		if method != "bead.update" {
			after.Body["claim"] = map[string]any{"holder": "a", "lease": "committed"}
		}
		if err := ValidateBeadWrite(method, before, after, c.Principal, c, []Action{Modify}); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string]any{"project": "other", "parent": ref("other"), "owner": "other", "assignee": "other", "type": "task", "labels": []any{"different"}, "restricted": false} {
		t.Run(key, func(t *testing.T) {
			after := bead(copyBody())
			after.Body[key] = value
			if ValidateBeadWrite("bead.update", before, after, c.Principal, c, nil) == nil {
				t.Fatal("permission-bearing change allowed")
			}
			after = bead(copyBody())
			delete(after.Body, key)
			if ValidateBeadWrite("bead.update", before, after, c.Principal, c, nil) == nil {
				t.Fatal("permission-bearing clear allowed")
			}
		})
	}
	for _, m := range []string{"bead.create", "bead.reparent", "project.rename", "project.delete", "admin.restore.begin", "tx.apply", "history.revert", "bead.setMeta"} {
		if ValidateBeadWrite(m, before, before, c.Principal, c, nil) == nil {
			t.Fatalf("unsupported %s allowed", m)
		}
	}
	if ValidateBeadWrite("bead.update", before, before, model.PrincipalRef{Kind: "plugin", ID: "p"}, c, nil) == nil {
		t.Fatal("deputy allowed")
	}
	if ValidateBeadWrite("bead.update", before, before, c.Principal, c, []Action{Modify, "transition"}) == nil {
		t.Fatal("unmapped ontology transition allowed")
	}
	for _, body := range []map[string]any{nil, {}, {"project": nil}, {"project": " padded"}, {"project": "web/api", "parent": map[string]any{"key": "new"}}, {"project": "web/api", "parent": map[string]any{"id": "p", "branch": 1}}, {"project": "web/api", "parent": json.RawMessage(`{"id":"p","unknown":1}`)}} {
		b := bead(body)
		if ValidateBeadWrite("bead.update", b, b, c.Principal, c, nil) == nil {
			t.Fatalf("invalid source admitted: %+v", body)
		}
	}
}
