package service

import (
	"encoding/hex"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Workflow items occupy a reserved namespace disjoint from ordinary bead IDs.
// Only the exact lowercase 128-bit item identifier on branch zero is exposed
// through the standard bead tools; run references are not bead references.
func validateWorkflowRef(ref model.Ref) error {
	if ref.Validate() != nil || !ref.IsID() || !ref.Branch.IsZero() || !strings.HasPrefix(ref.ID, workflowplugin.ItemPrefix) {
		return failure("invalid_params", "workflow items require a branch-zero workflow:item/<32 lowercase hex> reference")
	}
	suffix := strings.TrimPrefix(ref.ID, workflowplugin.ItemPrefix)
	raw, err := hex.DecodeString(suffix)
	if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != suffix {
		return failure("invalid_params", "workflow items require a branch-zero workflow:item/<32 lowercase hex> reference")
	}
	return nil
}

func validateWorkflowWrite(ref model.Ref, expect []model.Precondition, meta model.WriteMeta) error {
	if err := validateWorkflowRef(ref); err != nil {
		return err
	}
	if meta.DryRun || len(expect) != 0 {
		return failure("invalid_params", "workflow items do not support dryRun or expect")
	}
	if strings.TrimSpace(meta.Project) == "" {
		return failure("project_required", "supply a project")
	}
	return nil
}
