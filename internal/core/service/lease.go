package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Service) effectiveLease(ctx context.Context, id string, base *pipeline.ClaimLease) (*pipeline.ClaimLease, error) {
	if base == nil {
		return nil, nil
	}
	store, ok := s.store.(driver.CheckpointStore)
	if !ok {
		return base, nil
	}
	return pipeline.EffectiveLease(ctx, store, id, base)
}
