package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

type forcedCloseReplayOntology interface {
	ValidateForcedCloseReplay(context.Context, port.ProposedChange, port.SnapshotReader) error
}

func forcedCloseTargets(proposal port.ProposedChange) []model.ResourceRef {
	var targets []model.ResourceRef
	seen := map[string]bool{}
	for _, change := range proposal.Changes {
		if proposal.ForcesClose(change) && !seen[change.Resource.Ref.ID] {
			seen[change.Resource.Ref.ID] = true
			targets = append(targets, change.Resource)
		}
	}
	return targets
}
