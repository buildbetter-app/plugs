package service

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CompactionInput is detached input to a host-supplied summary. The core has
// no outbound client, credential, or implicit summarizer. Text contains only
// the fields that compaction will replace; title and unrelated data stay out.
type CompactionInput struct {
	Ref      model.Ref
	Revision model.Revision
	Tier     int
	ClosedAt time.Time
	Text     map[string]string
}

type CompactionSummaryProvider interface {
	Summary(context.Context, CompactionInput) (string, error)
}

type CompactionSummaryFunc func(context.Context, CompactionInput) (string, error)

func (f CompactionSummaryFunc) Summary(ctx context.Context, input CompactionInput) (string, error) {
	return f(ctx, input)
}

// CompactionConfig is internal host configuration, never RPC input. An absent
// threshold uses the adopted 30/90-day default; an explicit nonpositive value
// is invalid. VerifyPresence must validate an issued D32 proof bound to the
// method and digest; merely receiving a client-shaped object is insufficient.
type CompactionConfig struct {
	Summary        CompactionSummaryProvider
	ThresholdDays  map[int]int
	VerifyPresence func(context.Context, string, string, map[string]any) error
}

func WithCompaction(config CompactionConfig) Option {
	thresholds := make(map[int]int, len(config.ThresholdDays))
	for tier, days := range config.ThresholdDays {
		thresholds[tier] = days
	}
	config.ThresholdDays = thresholds
	return func(s *Service) { s.compaction = config }
}
