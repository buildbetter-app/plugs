package authzrpc

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var _ service.HistoryReader = (*GuardedService)(nil)

// historyRead authorizes the current record, never its historical project or
// assignment. Memory keeps the domain service's separate current-owner and
// lifetime checks; it must not be treated as a bead by the fixed checker.
func (s *GuardedService) historyRead(ctx context.Context, by model.PrincipalRef, ref model.Ref) (service.HistoryReader, string, error) {
	if err := ref.Validate(); err != nil {
		return nil, "", &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "invalid history reference"}}
	}
	current, snapshot, closeReader, err := s.currentAsOf(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	defer closeReader()
	reader, ok := s.delegate.(service.HistoryReader)
	if !ok {
		return nil, "", &model.Error{Data: model.ErrorData{Code: "unsupported", Hint: "history reader unavailable"}}
	}
	rows, err := current.Get(ctx, []model.Ref{ref})
	if err != nil {
		return nil, "", err
	}
	if len(rows) != 1 {
		return nil, "", notFound()
	}
	switch rows[0].Kind {
	case "memory":
		// HistoryGet/Diff enforce the installed memory authorization boundary
		// before reading and again before returning, including lifetime checks.
		return reader, "", nil
	case "bead":
		allowed, err := s.checker.readDecision(ctx, snapshot, by, "bead.get", ref)
		if err != nil {
			return nil, "", err
		}
		if !allowed {
			return nil, "", notFound()
		}
		binding, err := s.checker.ReadAuthorityBinding(ctx, by, snapshot)
		return reader, binding, err
	default:
		return nil, "", notFound()
	}
}

func (s *GuardedService) HistoryGet(ctx context.Context, by model.PrincipalRef, req service.HistoryVersionRequest) (service.HistoryVersion, error) {
	reader, binding, err := s.historyRead(ctx, by, req.Ref)
	if err != nil {
		return service.HistoryVersion{}, err
	}
	result, err := reader.HistoryGet(ctx, by, req)
	if err != nil {
		return service.HistoryVersion{}, err
	}
	if binding != "" {
		if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
			return service.HistoryVersion{}, err
		}
	}
	return result, nil
}

func (s *GuardedService) HistoryDiff(ctx context.Context, by model.PrincipalRef, req service.HistoryDiffRequest) (service.HistoryDiff, error) {
	reader, binding, err := s.historyRead(ctx, by, req.Ref)
	if err != nil {
		return service.HistoryDiff{}, err
	}
	result, err := reader.HistoryDiff(ctx, by, req)
	if err != nil {
		return service.HistoryDiff{}, err
	}
	if binding != "" {
		if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
			return service.HistoryDiff{}, err
		}
	}
	return result, nil
}

// currentAsOf resolves history content and Get's explicit instant selector.
// The shared current reader remains identical to main for claim-next callers.
func (s *GuardedService) currentAsOf(ctx context.Context, asOf *service.AsOf) (driver.Reader, Snapshot, func(), error) {
	if s == nil || s.checker == nil || s.checker.store == nil || s.lease == nil {
		return nil, nil, func() {}, errors.New("authorization store is unavailable")
	}
	// Authorization and idempotency reads can precede the domain service's
	// lease guard. Fence them before touching storage after lease loss too.
	select {
	case <-s.lease.Lost():
		return nil, nil, func() {}, &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "active storage lease was lost"}}
	default:
	}
	reader, err := service.ReadAsOf(ctx, s.checker.store, asOf)
	if err != nil {
		return nil, nil, func() {}, err
	}
	return reader, reader, func() { _ = reader.Close() }, nil
}
