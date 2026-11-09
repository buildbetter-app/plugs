package authzrpc

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// These constructors exist only in the test build, never the server API.
func NewWithClockForTest(ctx context.Context, config Config, now func() time.Time) (*Composition, error) {
	return newWithClock(ctx, config, now)
}

func AddRuleForTest(composition *Composition, rules authz.RuleSet) {
	composition.Checker.chain.Rules = append(composition.Checker.chain.Rules, rules)
}

// AfterFirstReadyPageForTest inserts a barrier after one actual domain page
// has closed its reader and before the guarded aggregate requests its next.
// It changes no production constructor or writer and supplies no fake page.
func AfterFirstReadyPageForTest(composition *Composition, after func(service.BeadPage)) {
	composition.Service.delegate = &readyPageBarrier{Service: composition.Service.delegate, after: after}
}

type readyPageBarrier struct {
	rpc.Service
	after func(service.BeadPage)
}

func (b *readyPageBarrier) ReadyWithFilter(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest, filter service.ReadyAuthorizer) (service.BeadPage, error) {
	delegate := b.Service.(interface {
		ReadyWithFilter(context.Context, model.PrincipalRef, service.ReadyRequest, service.ReadyAuthorizer) (service.BeadPage, error)
	})
	page, err := delegate.ReadyWithFilter(ctx, by, req, filter)
	if err == nil && b.after != nil {
		after := b.after
		b.after = nil
		after(page)
	}
	return page, err
}
