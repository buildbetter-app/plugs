package authzrpc

import (
	"context"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/claimrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type claimMaintenance interface {
	claimrpc.Service
	HeartbeatWithOutcome(context.Context, model.PrincipalRef, service.HeartbeatRequest, model.WriteMeta) (port.HeartbeatOutcome, error)
}

var _ claimMaintenance = (*GuardedService)(nil)
var _ pipeline.HeartbeatReplayAuthorizer = (*Checker)(nil)

// claimMaintenanceSlice reuses only successful dependency registrations within
// one Check on one pinned slice. Reads, policies and per-change checks still run;
// the first registration always reaches the real snapshot's commit guard.
type claimMaintenanceSlice struct {
	authz.Slice
	mu       sync.Mutex
	recorded map[authz.Dependency]struct{}
}

func (s *claimMaintenanceSlice) Record(ctx context.Context, dependency authz.Dependency) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.recorded[dependency]; ok {
		return nil
	}
	if err := s.Slice.Record(ctx, dependency); err != nil {
		return err
	}
	s.recorded[dependency] = struct{}{}
	return nil
}

func (c *Checker) claimMaintenanceChain() authz.Chain {
	chain := c.chain
	chain.Rules = make([]authz.RuleSet, len(c.chain.Rules))
	for i, rules := range c.chain.Rules {
		chain.Rules[i] = rules
		if fixed, ok := rules.(*authzfixed.RuleSet); ok {
			chain.Rules[i] = claimFixedRules{fixed}
		}
	}
	return chain
}

type claimFixedRules struct{ fixed *authzfixed.RuleSet }

func (r claimFixedRules) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	mapped := append([]authz.CheckRequest(nil), requests...)
	for i := range mapped {
		q := &mapped[i]
		if q.Action == authz.Modify && q.Resource.Kind == authz.Bead &&
			(q.Method == "bead.heartbeat" || q.Method == "bead.release") {
			q.Method = "bead.update"
		}
	}
	return r.fixed.Eval(ctx, s, mapped)
}

func (claimFixedRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

// validateHeartbeatClaim runs only after the installed chain has admitted the
// resource. Before and replay both require a current held base lease: an old
// published receipt must not bypass release, reassignment or expiry. The Writer
// still owns atomic record/checkpoint publication and its storage lease fence.
func (c *Checker) validateHeartbeatClaim(ctx context.Context, by model.PrincipalRef, ref model.Ref, row driver.Record) error {
	if c == nil || c.now == nil || c.lease == nil {
		return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil || doc.ID != ref.ID {
		return &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "invalid persisted bead"}}
	}
	if doc.Lease == nil {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "bead has no active claim"}}
	}
	epoch, err := doc.Lease.Epoch.Uint64()
	if err != nil || epoch == 0 || epoch > c.lease.Epoch() {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "claim has an invalid storage lease epoch"}}
	}
	ttl, err := doc.Lease.TtlMs.Int64()
	if err != nil || ttl < 1 || ttl > int64((1<<63-1)/time.Millisecond) {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "claim has an invalid lease TTL"}}
	}
	checkpoints, ok := c.store.(driver.CheckpointStore)
	if !ok {
		return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	effective, err := pipeline.EffectiveLease(ctx, checkpoints, ref.ID, doc.Lease)
	if err != nil {
		return err
	}
	principal, principalErr := driver.PrincipalKey(by)
	holder, holderErr := driver.PrincipalKey(effective.Principal)
	if principalErr != nil || holderErr != nil || holder != principal {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "bead is claimed by another principal"}}
	}
	if !c.now().UTC().Before(effective.ExpiresAt) {
		return &model.Error{Data: model.ErrorData{Code: "lease_expired", Hint: "bead claim has expired"}}
	}
	return nil
}

// AuthorizeHeartbeatReplay checks the receipt's original base claim after the
// ordinary current-authority/holder/deadline checks. The current overlay may
// have advanced; a valid same-claim replay still returns its original deadline.
func (c *Checker) AuthorizeHeartbeatReplay(ctx context.Context, by model.PrincipalRef, ref model.Ref, original *pipeline.ClaimLease, reader port.SnapshotReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader == nil {
		return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if by.Kind != "agent" {
		return forbidden("public claim maintenance requires an Agent")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return forbidden("heartbeat requires an existing authorized bead")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.ID != ref.ID {
		return &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "invalid persisted bead"}}
	}
	if original == nil || original.ExpiresAt.IsZero() || doc.Lease == nil {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "heartbeat reservation belongs to a different claim"}}
	}
	principal, principalErr := driver.PrincipalKey(doc.Lease.Principal)
	bound, boundErr := driver.PrincipalKey(original.Principal)
	if principalErr != nil || boundErr != nil || principal != bound ||
		doc.Lease.Agent != original.Agent || doc.Lease.Epoch.String() != original.Epoch.String() ||
		!doc.Lease.ExpiresAt.Equal(original.ExpiresAt) {
		return &model.Error{Data: model.ErrorData{Code: "lease_not_held", Hint: "heartbeat reservation belongs to a different claim"}}
	}
	return nil
}

// publicClaims never admits the private system:core recovery principal. The
// installed Writer checks the current fixed modify permit and the actual claim
// holder; neither RPC metadata nor a caller-supplied agent label selects recovery.
func (s *GuardedService) publicClaims(ctx context.Context, by model.PrincipalRef) (claimMaintenance, error) {
	if s == nil || s.delegate == nil || s.checker == nil || s.lease == nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-s.lease.Lost():
		return nil, &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "active storage lease was lost"}}
	default:
	}
	if by.Kind != "agent" || by.Sponsor == nil || by.Sponsor.Kind != "user" {
		return nil, forbidden("claim maintenance requires an authenticated sponsored agent")
	}
	delegate, ok := s.delegate.(claimMaintenance)
	if !ok {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	return delegate, nil
}

func (s *GuardedService) HeartbeatWithOutcome(ctx context.Context, by model.PrincipalRef, req service.HeartbeatRequest, meta model.WriteMeta) (port.HeartbeatOutcome, error) {
	delegate, err := s.publicClaims(ctx, by)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	return delegate.HeartbeatWithOutcome(ctx, by, req, meta)
}

func (s *GuardedService) ReleaseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ReleaseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	delegate, err := s.publicClaims(ctx, by)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	return delegate.ReleaseWithOutcome(ctx, by, req, meta)
}
