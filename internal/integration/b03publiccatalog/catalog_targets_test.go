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

func TestB03ProjectCatalogExactTargetsHistoryAndSQLReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := b03NewPublic(t, engine)
			left, right := "b03-review", "b03-delivery"
			a, b := b03TargetDocument(ontology.Active), b03TargetDocument(ontology.Done)
			sourceRevisions := []model.Revision{f.row(t, f.source).Revision}
			sourceRevisions = append(sourceRevisions, b03InstallProject(t, f, left, a), b03InstallProject(t, f, right, b))
			b03ProjectCatalog(t, f, left, a)
			b03ProjectCatalog(t, f, right, b)
			var points []b03TargetPoint
			for _, project := range []string{left, right} {
				for _, initial := range []string{b03Active, b03WIP, b03Frozen} {
					c := b03TargetWrite(t, f, project, "bead.create", map[string]any{"type": "b03-exact", "targetStatus": initial, "fields": map[string]any{"title": "catalog exact " + initial}})
					if len(c.Results) != 1 {
						t.Fatal("exact create did not return one identity")
					}
					id := c.Results[0].Ref
					created := b03TargetObserve(t, f, project, id, "created-"+initial, initial, initial == b03Active)
					points = append(points, created)
					b03TargetRefusal(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": "unconfigured"})
					b03TargetRefusal(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": b03WIP})
					b03TargetWrite(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": b03Done})
					closed := b03TargetObserve(t, f, project, id, "first-close", b03Done, false)
					b03TargetWrite(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": b03Done})
					repeated := b03TargetObserve(t, f, project, id, "repeat-close", b03Done, false)
					b03TargetRefusal(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": "unconfigured"})
					b03TargetRefusal(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": b03Frozen})
					b03TargetWrite(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": b03Active})
					reopened := b03TargetObserve(t, f, project, id, "exact-reopen", b03Active, true)
					if closed.Closed == nil || closed.Closed.IsZero() || closed.Closed.Before(created.Created) || !reflect.DeepEqual(closed.Closed, repeated.Closed) || reopened.Closed != nil {
						t.Fatal("exact close/repeat/reopen did not preserve first close and clear it only at reopen")
					}
					for _, point := range []b03TargetPoint{closed, repeated, reopened} {
						if !point.Created.Equal(created.Created) || !reflect.DeepEqual(point.Started, created.Started) {
							t.Fatal("close/reopen rewrote created or started time")
						}
					}
					points = append(points, closed, repeated, reopened)
					b03TargetHistoricalStatus(t, f, project, id, created.Revision, initial)
					b03TargetHistoricalStatus(t, f, project, id, closed.Revision, b03Done)
					b03TargetHistoricalStatus(t, f, project, id, reopened.Revision, b03Active)
				}
				// This reaches catalog membership and project/category checking;
				// a generic patch's use-slot refusal is a different old control.
				b03TargetRefusal(t, f, project, "bead.create", map[string]any{"type": "b03-exact", "targetStatus": "unconfigured", "fields": map[string]any{"title": "must refuse"}})
			}
			local := b03TargetWrite(t, f, left, "bead.create", map[string]any{"type": "b03-exact", "targetStatus": "project_local", "fields": map[string]any{"title": "same key active in review"}}).Results[0].Ref
			points = append(points, b03TargetObserve(t, f, left, local, "project-local-active", "project_local", true))
			b03TargetRefusal(t, f, right, "bead.create", map[string]any{"type": "b03-exact", "targetStatus": "project_local", "fields": map[string]any{"title": "same key done in delivery"}})
			b03TargetSourceHistory(t, f, left, sourceRevisions)
			b03TargetSourceHistory(t, f, right, sourceRevisions)
			var unchanged ontologyops.DiffResult
			f.call(t, "ontology.diff", map[string]any{"project": left, "fromRevision": sourceRevisions[1], "toRevision": sourceRevisions[2]}, false, "", &unchanged)
			if len(unchanged.Patch) != 0 {
				t.Fatal("another project's source update changed review's effective historical catalog")
			}
			if engine != "memory" {
				before := f.snapshot(t)
				f.close(t)
				f.start(t)
				f.bind(t)
				if !bytes.Equal(before, f.snapshot(t)) {
					t.Fatal("actual SQL close/open changed durable public catalog/lifecycle state")
				}
				b03ProjectCatalog(t, f, left, a)
				b03ProjectCatalog(t, f, right, b)
				b03TargetSourceHistory(t, f, left, sourceRevisions)
				for _, p := range points {
					b03TargetHistoricalStatus(t, f, p.Project, model.Ref{ID: p.ID}, p.Revision, p.Status)
				}
			}
			b03Write(t, "b03-project-targets-"+engine, map[string]any{"scope": "PUBLIC_PLUG_PROJECT_CATALOG_NOT_NATIVE_ADMISSION", "complete": !t.Failed(), "source_revisions": sourceRevisions, "points": points, "claims": "EXCLUDED"})
		})
	}
}
