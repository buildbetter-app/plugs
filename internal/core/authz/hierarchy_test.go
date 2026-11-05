package authz

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func hierarchyBead(id, project string, parent *model.Ref) model.Record {
	body := map[string]any{"id": id, "type": "issue", "project": project, "fields": map[string]any{"title": "task"}}
	if parent != nil {
		body["parent"] = *parent
	}
	return model.Record{Kind: "bead", Ref: ref(id), Revision: model.Natural64(1), Body: body}
}

func TestHierarchyImageAndMoveGuard(t *testing.T) {
	caller := Caller{Principal: model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "operator"}}}
	old, next := ref("old"), ref("new")
	before := hierarchyBead("child", "child-project", &old)
	after := hierarchyBead("child", "child-project", &next)
	for _, parent := range []*model.Ref{&next, nil} {
		after = hierarchyBead("child", "child-project", parent)
		got, err := ValidateHierarchyWrite(model.OpReparent, &before, after, caller.Principal, caller)
		if err != nil || got.Project != "child-project" || (got.Parent == nil) != (parent == nil) {
			t.Fatalf("move/detach failed: %v", err)
		}
	}
	for _, key := range []string{"project", "type", "owner", "assignee", "labels", "restricted"} {
		t.Run(key, func(t *testing.T) {
			after := hierarchyBead("child", "child-project", &next)
			after.Body[key] = "changed"
			if _, err := ValidateHierarchyWrite(model.OpReparent, &before, after, caller.Principal, caller); err == nil {
				t.Fatal("permission-bearing move escaped its child's source")
			}
		})
	}
	for _, kind := range []model.OpKind{model.OpPatch, model.OpDelete, model.OpLink} {
		if _, err := ValidateHierarchyWrite(kind, &before, after, caller.Principal, caller); err == nil {
			t.Fatal("unmapped operation allowed")
		}
	}
	if _, err := ValidateHierarchyWrite(model.OpReparent, nil, after, caller.Principal, caller); err == nil {
		t.Fatal("move without source allowed")
	}
	if _, err := ValidateHierarchyWrite(model.OpCreate, &before, after, caller.Principal, caller); err == nil {
		t.Fatal("create over an existing source allowed")
	}
	if _, err := ValidateHierarchyWrite(model.OpReparent, &before, after, model.PrincipalRef{Kind: "plugin", ID: "deputy"}, caller); err == nil {
		t.Fatal("deputy allowed")
	}
	user := Caller{Principal: *caller.Principal.Sponsor}
	if _, err := ValidateHierarchyWrite(model.OpReparent, &before, after, user.Principal, user); err == nil {
		t.Fatal("User write policy was broadened")
	}
	// The old path must remain incapable of changing a parent.
	if err := ValidateBeadWrite("bead.update", before, hierarchyBead("child", "child-project", &next), caller.Principal, caller, []Action{Modify}); err == nil {
		t.Fatal("ordinary update gained a parent transition")
	}
}

func TestHierarchyCreateAndMalformedImages(t *testing.T) {
	caller := Caller{Principal: model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "operator"}}}
	parent := ref("parent")
	after := hierarchyBead("child", "child-project", &parent)
	if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err != nil {
		t.Fatal(err)
	}
	after.Body["createdAt"] = "2026-11-03T00:00:00.000000001Z"
	if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err != nil {
		t.Fatalf("valid core creation instant refused: %v", err)
	}
	for _, mutate := range []func(*model.Record){
		func(r *model.Record) { r.Body["id"] = "other" },
		func(r *model.Record) { delete(r.Body, "project") },
		func(r *model.Record) { r.Body["project"] = " padded" },
		func(r *model.Record) { r.Body["fields"] = nil },
		func(r *model.Record) { r.Body["parent"] = ref("child") },
		func(r *model.Record) { r.Body["parent"] = model.RefByKey("future") },
		func(r *model.Record) { r.Body["parent"] = model.RefByID("parent", model.Natural64(1)) },
		func(r *model.Record) { r.Body["parent"] = map[string]any{"id": "parent", "unknown": "secret"} },
		func(r *model.Record) { r.Body["createdAt"] = nil },
		func(r *model.Record) { r.Body["createdAt"] = 1 },
		func(r *model.Record) { r.Body["createdAt"] = "not-an-instant" },
		func(r *model.Record) { r.Body["createdAt"] = "0001-01-01T00:00:00Z" },
		func(r *model.Record) { r.Body["unknown"] = true },
		func(r *model.Record) { r.Body["startedAt"] = "2026-11-03T00:00:00Z" },
		func(r *model.Record) { r.Body["closedAt"] = "2026-11-03T00:00:00Z" },
		func(r *model.Record) { r.Body["owner"] = "operator" },
		func(r *model.Record) { r.Body["labels"] = []string{"private"} },
		func(r *model.Record) { r.Kind = "memory" },
	} {
		after := hierarchyBead("child", "child-project", &parent)
		mutate(&after)
		if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err == nil {
			t.Fatalf("malformed or protected creation allowed: %+v", after)
		}
	}
}

func TestHierarchyCreateAcceptsOnlyCanonicalCoreCreationInstant(t *testing.T) {
	caller := Caller{Principal: model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "operator"}}}
	after := hierarchyBead("child", "child-project", nil)
	after.Body["createdAt"] = "2026-10-29T12:00:00.123456789Z"
	if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err != nil {
		t.Fatalf("trusted core timestamp refused: %v", err)
	}
	for _, invalid := range []any{nil, true, 1, "", "not-an-instant", "0001-01-01T00:00:00Z", "2026-10-29T12:00:00+00:00", "2026-10-29T13:00:00+01:00"} {
		after.Body["createdAt"] = invalid
		if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err == nil {
			t.Fatalf("invalid core timestamp accepted: %#v", invalid)
		}
	}
	for _, unsupported := range []string{"startedAt", "closedAt", "owner", "assignee", "labels", "restricted", "unknown"} {
		after := hierarchyBead("child", "child-project", nil)
		after.Body["createdAt"] = "2026-10-29T12:00:00Z"
		after.Body[unsupported] = "2026-10-29T12:00:00Z"
		if _, err := ValidateHierarchyWrite(model.OpCreate, nil, after, caller.Principal, caller); err == nil {
			t.Fatalf("core timestamp broadened unsupported assignment %q", unsupported)
		}
	}
}
