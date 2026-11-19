package cardb20

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This ordinary public control is separate from the admitted native archive
// controls. No source archive, source revision or native outcome is fabricated
// by installing these explicitly declared Plug project overlays.
func b03TargetDocument(category ontology.Category) ontology.Document {
	d := ontology.DefaultDocument()
	for name, c := range b03Categories {
		s := ontology.Status{Name: name, Category: c}
		if c == ontology.Done {
			s.Outcome = ontology.Success
		}
		d.Statuses[name] = s
	}
	d.Statuses["project_local"] = ontology.Status{Name: "project_local", Category: category}
	if category == ontology.Done {
		s := d.Statuses["project_local"]
		s.Outcome = ontology.Success
		d.Statuses["project_local"] = s
	}
	states := []string{b03Active, b03WIP, b03Done, b03Frozen, "project_local"}
	w := ontology.Workflow{Initial: b03Active, Statuses: states,
		Slots: map[ontology.Slot]string{ontology.Claim: b03WIP, ontology.Close: b03Done, ontology.Reopen: b03Active, ontology.Defer: b03Frozen},
	}
	for _, name := range states {
		guards := []ontology.Guard{{Kind: ontology.Permission}}
		if d.Statuses[name].Category == ontology.Done {
			guards = []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}
		}
		w.Transitions = append(w.Transitions, ontology.Rule{To: name, Guards: guards})
	}
	d.Workflows["b03-exact"] = w
	d.Types["b03-exact"] = ontology.Type{Name: "B03 exact catalog", Workflow: "b03-exact", Fields: map[string]ontology.Field{"title": {Type: "string"}}}
	return d
}

func b03ProjectMeta(project string) map[string]any {
	return map[string]any{"project": project, "agent": "b03-catalog-targets"}
}

func b03InstallProject(t *testing.T, f *b03Public, project string, d ontology.Document) model.Revision {
	t.Helper()
	var p ontologyops.PlanResult
	f.call(t, "ontology.plan", map[string]any{"project": project, "document": d}, false, "", &p)
	if !p.Changes.Ready || len(p.Violations) != 0 || p.PlanDigest == "" {
		t.Fatalf("public project catalog plan not ready: %+v", p)
	}
	var commit model.Commit
	meta := b03ProjectMeta(project)
	meta["idempotencyKey"] = b03TargetKey(f.sequence + 1)
	f.call(t, "ontology.apply", map[string]any{"_meta": meta, "planDigest": p.PlanDigest, "expect": p.Changes.Expect,
		"humanPresence": map[string]any{"mode": "presence_check", "challenge": p.PlanDigest, "proof": p.PlanDigest}}, true, "", &commit)
	return commit.Revision
}

func b03ProjectCatalog(t *testing.T, f *b03Public, project string, want ontology.Document) ontologyops.Result {
	t.Helper()
	var got ontologyops.Result
	// Deliberately put a different project in _meta. The explicit public
	// project field, rather than the host default or another overlay, wins.
	f.call(t, "ontology.get", map[string]any{"project": project, "_meta": b03ProjectMeta("b03-other-default")}, false, "", &got)
	if got.Drifted || got.Document == nil || !reflect.DeepEqual(*got.Document, want) || got.Revision.IsZero() {
		t.Fatalf("project %s installed catalog/source revision differs: %+v", project, got)
	}
	return got
}

func b03TargetWrite(t *testing.T, f *b03Public, project, method string, params map[string]any) model.Commit {
	t.Helper()
	params["_meta"] = b03ProjectMeta(project)
	// b03Public.call owns the sequence. An explicitly supplied _meta must
	// carry its key because the helper preserves the caller's whole object.
	params["_meta"].(map[string]any)["idempotencyKey"] = b03TargetKey(f.sequence + 1)
	var commit model.Commit
	f.call(t, method, params, true, "", &commit)
	if commit.Revision.IsZero() {
		t.Fatal("ordinary public mutation returned no committed revision")
	}
	return commit
}

func b03TargetKey(sequence int) string {
	return fmt.Sprintf("%032x", sequence)
}

type b03TargetPoint struct {
	Phase, Project, ID, Status string
	Revision                   model.Revision
	Created                    time.Time
	Started, Closed            *time.Time
	Ready                      bool
}

func b03TargetObserve(t *testing.T, f *b03Public, project string, id model.Ref, phase, status string, ready bool) b03TargetPoint {
	t.Helper()
	var get struct {
		Bead struct {
			ID, Project, Status string
			Revision            model.Revision
		}
	}
	f.call(t, "bead.get", map[string]any{"id": id, "_meta": b03ProjectMeta(project)}, false, "", &get)
	var page struct {
		Items      []struct{ ID, Project string }
		NextCursor string
	}
	f.call(t, "bead.ready", map[string]any{"limit": 1000, "_meta": b03ProjectMeta(project)}, false, "", &page)
	found := false
	for _, item := range page.Items {
		if item.Project != project {
			t.Fatal("project readiness leaked a different project")
		}
		found = found || item.ID == id.ID
	}
	row := f.row(t, id)
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil || doc.CreatedAt == nil || doc.CreatedAt.IsZero() || doc.Project != project || doc.Fields["status"] != status || get.Bead.ID != id.ID || get.Bead.Project != project || get.Bead.Status != status || get.Bead.Revision != row.Revision || found != ready || page.NextCursor != "" {
		t.Fatalf("%s exact public status/committed document/readiness differs: %+v %+v %v", phase, get, doc, err)
	}
	return b03TargetPoint{phase, project, id.ID, status, row.Revision, *doc.CreatedAt, doc.StartedAt, doc.ClosedAt, found}
}

func b03TargetRefusal(t *testing.T, f *b03Public, project, method string, params map[string]any) {
	t.Helper()
	before := f.snapshot(t)
	params["_meta"] = b03ProjectMeta(project)
	params["_meta"].(map[string]any)["idempotencyKey"] = b03TargetKey(f.sequence + 1)
	f.call(t, method, params, true, "invalid_params", nil)
	if !bytes.Equal(before, f.snapshot(t)) {
		t.Fatal("refused exact catalog target changed bounded complete rows/meta/journal snapshot")
	}
}

func b03TargetSourceHistory(t *testing.T, f *b03Public, project string, revisions []model.Revision) {
	t.Helper()
	var h ontologyops.HistoryResult
	f.call(t, "ontology.history", map[string]any{"project": project, "limit": 1000}, false, "", &h)
	if len(h.Revisions) != len(revisions) {
		t.Fatalf("actual source history revisions=%+v want=%+v", h.Revisions, revisions)
	}
	for i, change := range h.Revisions {
		if change.Revision != revisions[i] || len(change.Changes) != 1 || (change.Changes[0].Ref.ID != f.source.ID || change.Changes[0].Ref.Branch.String() != f.source.Branch.String()) {
			t.Fatalf("source history includes an ordinary row or loses source identity: %+v", change)
		}
	}
}

func b03TargetHistoricalStatus(t *testing.T, f *b03Public, project string, id model.Ref, at model.Revision, status string) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var get struct {
		Bead struct{ ID, Project, Status string }
	}
	f.call(t, "bead.get", map[string]any{"id": id, "asOf": map[string]any{"revision": at, "incarnation": meta.Incarnation}, "_meta": b03ProjectMeta(project)}, false, "", &get)
	if get.Bead.ID != id.ID || get.Bead.Project != project || get.Bead.Status != status {
		t.Fatalf("historical public status at %s differs: %+v", at.String(), get)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{id})
	if err != nil || len(rows) != 1 {
		t.Fatalf("historical committed document missing: %v", err)
	}
	d, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || d.Fields["status"] != status {
		t.Fatalf("historical committed/public status disagree: %+v %v", d, err)
	}
}
