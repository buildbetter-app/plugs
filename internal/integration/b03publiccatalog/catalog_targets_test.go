package cardb20

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
)

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
