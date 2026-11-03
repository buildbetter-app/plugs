package rpc

import (
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// PlanLabelRenameFromResults adapts complete public bead.get RESULT objects to
// the adopted client migration plan. It is not a new RPC method or operation.
// Submit the returned ChangeSet through the existing tx.apply descriptor with
// ordinary caller metadata; no planned count is a successful write receipt.
//
// Input must come from the caller's explicitly enumerated durable scope, in
// one store/incarnation. This adapter cannot certify a global scope, authorize
// a write, provide cross-incarnation replay, or silently retry stale revisions.
func PlanLabelRenameFromResults(project, oldLabel, newLabel string, results []json.RawMessage) (service.LabelRenamePlan, error) {
	if len(results) > 1000 {
		return service.LabelRenamePlan{}, labelRenameInputError("scope_too_large", "label migration requires at most 1000 selected beads")
	}
	beads := make([]service.Bead, 0, len(results))
	for _, raw := range results {
		value, err := wire.DecodeJSON(raw)
		if err != nil {
			return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "label migration requires a valid bead.get result")
		}
		result, ok := value.(map[string]any)
		if !ok {
			return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "label migration requires a bead.get result object")
		}
		projection, ok := result["bead"].(map[string]any)
		if !ok {
			return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "label migration requires a complete bead projection")
		}
		bead := service.Bead{}
		bead.ID, _ = projection["id"].(string)
		bead.Type, _ = projection["type"].(string)
		bead.Project, _ = projection["project"].(string)
		bead.Fields, _ = projection["fields"].(map[string]any)
		revision, ok := projection["revision"].(json.Number)
		if !ok || bead.Revision.UnmarshalJSON([]byte(revision)) != nil {
			return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "label migration requires a lossless bead revision")
		}
		if rawLabels, present := projection["labels"]; present {
			labels, ok := rawLabels.([]any)
			if !ok {
				return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "projected labels must be an array of strings")
			}
			for _, rawLabel := range labels {
				label, ok := rawLabel.(string)
				if !ok {
					return service.LabelRenamePlan{}, labelRenameInputError("invalid_params", "projected labels must be an array of strings")
				}
				bead.Labels = append(bead.Labels, label)
			}
		}
		beads = append(beads, bead)
	}
	return service.PlanLabelRename(project, oldLabel, newLabel, beads)
}

func labelRenameInputError(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint}}
}
