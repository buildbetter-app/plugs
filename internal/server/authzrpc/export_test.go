package authzrpc

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
)

// These constructors exist only in the test build, never the server API.
func NewWithClockForTest(ctx context.Context, config Config, now func() time.Time) (*Composition, error) {
	return newWithClock(ctx, config, now)
}

func AddRuleForTest(composition *Composition, rules authz.RuleSet) {
	composition.Checker.chain.Rules = append(composition.Checker.chain.Rules, rules)
}
