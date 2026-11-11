package b01readyorder_test

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// A workflow parent can be excluded from the returned ephemeral plane while
// still being the actual typed ancestor of a durable child. The parent
// predicate must traverse that checkpoint edge before it applies resources,
// priority, or a page limit.
func TestB01WorkflowAncestorSurvivesDurableOnlyParentSelection(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptance(t, engine, true)
			parentItems := p.invoke(t, "durable-only-parent", []map[string]any{{
				"label": "workflow-parent", "title": "workflow parent", "type": "issue",
				"fields": map[string]any{"priority": 1},
			}})
			parent := model.RefByID(parentItems[0].ID, model.Natural64(0))
			childItems := p.invoke(t, "durable-only-child", []map[string]any{{
				"label": "workflow-child", "title": "workflow child", "type": "issue",
				"parent": parent, "fields": map[string]any{"priority": 0},
			}})
			workflowChild := model.RefByID(childItems[0].ID, model.Natural64(0))
			durable := p.create(t, "durable-under-workflow", "issue", "", 2, nil)

			// This is the public mixed-plane parent relation. It updates the
			// provider checkpoint; it does not synthesize a durable Parent field.
			linkParams := map[string]any{
				"from": durable, "to": parent, "kind": "parent-child",
				"_meta": map[string]any{"project": project, "idempotencyKey": fixtureKey("durable-only-parent-link")},
			}
			link := p.call(t, "bead.link", linkParams)
			var commit model.Commit
			if link.Error != nil || json.Unmarshal(link.Result, &commit) != nil || !commit.Revision.IsZero() || len(commit.Results) != 1 || commit.Results[0].Ref != parent || commit.Results[0].Revision.String() != "1" {
				t.Fatalf("public workflow-parent relation lost its provider commit: error=%+v result=%s", link.Error, link.Result)
			}

			// Included reads select the workflow child through its workflow edge.
			p.point(t, "workflow-edge-included", map[string]any{
				"parent": parent.ID, "resources": []string{workflowChild.ID},
			}, workflowChild.ID)
			// Excluding ephemeral output must still use the same workflow parent
			// edge to select the ordinary child before pagination.
			p.point(t, "workflow-edge-retained-for-durable-output", map[string]any{
				"parent": parent.ID, "includeEphemeral": false, "resources": []string{durable.ID},
			}, durable.ID)
		})
	}
}
