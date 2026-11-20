package service

import (
	"context"
	"errors"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadBatchAuthorizer is the request-scoped bridge used by view consumers
// whose legacy point callback cannot expose a batch field. The session is
// created for one Run call and never escapes its context.
type ReadBatchAuthorizer func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error)

type readBatchContextKey struct{}

// ReadBatchSession holds one finite set of resources and its decisions for a
// single view request. It is deliberately not a cache: no session is reused
// across calls, and an authority error is retained as an error for the rest of
// this request rather than becoming an absence.
type ReadBatchSession struct {
	mu           sync.Mutex
	resources    []model.ResourceRef
	principalKey string
	started      bool
	ready        chan struct{}
	decisions    map[string]bool
	known        map[string]struct{}
	err          error
}

func WithReadBatchSession(ctx context.Context, resources []model.ResourceRef) (context.Context, *ReadBatchSession) {
	copyResources := append([]model.ResourceRef(nil), resources...)
	session := &ReadBatchSession{resources: copyResources}
	return context.WithValue(ctx, readBatchContextKey{}, session), session
}

// ReadBatchFromContext returns the request-local bridge, if a view runner
// installed one on this context.
func ReadBatchFromContext(ctx context.Context) *ReadBatchSession {
	if ctx == nil {
		return nil
	}
	session, _ := ctx.Value(readBatchContextKey{}).(*ReadBatchSession)
	return session
}

// Resolve runs the trusted batch authorizer once, then returns the decision
// for ref. A resource outside the declared batch is left to the ordinary
// point path, which preserves blocker and non-candidate behavior.
func (s *ReadBatchSession) Resolve(ctx context.Context, by model.PrincipalRef, authorize ReadBatchAuthorizer, resource model.ResourceRef) (bool, bool, error) {
	if s == nil || authorize == nil {
		return false, false, nil
	}
	principalKey, err := driver.PrincipalKey(by)
	if err != nil {
		return false, true, err
	}

	s.mu.Lock()
	declared := false
	for _, candidate := range s.resources {
		if candidate.Kind == resource.Kind && candidate.Ref.ID == resource.Ref.ID && candidate.Ref.Branch == resource.Ref.Branch {
			declared = true
			break
		}
	}
	if !declared {
		s.mu.Unlock()
		return false, false, nil
	}
	if s.principalKey == "" {
		s.principalKey = principalKey
	} else if s.principalKey != principalKey {
		s.mu.Unlock()
		return false, true, errors.New("read authorization session principal changed")
	}
	if s.known != nil {
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return false, true, err
		}
		allowed, ok := s.decisions[resource.Ref.ID]
		authErr := s.err
		s.mu.Unlock()
		if !ok {
			return false, true, authErr
		}
		return allowed, true, authErr
	}
	if s.started {
		ready := s.ready
		s.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return false, true, ctx.Err()
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return false, true, err
		}
		allowed, ok := s.decisions[resource.Ref.ID]
		authErr := s.err
		s.mu.Unlock()
		if !ok {
			return false, true, authErr
		}
		return allowed, true, authErr
	}
	s.started = true
	s.ready = make(chan struct{})
	resources := append([]model.ResourceRef(nil), s.resources...)
	ready := s.ready
	s.mu.Unlock()

	decisions, authErr := authorize(ctx, by, resources)
	s.mu.Lock()
	if authErr == nil && len(decisions) != len(resources) {
		authErr = errors.New("read authorization session returned incomplete decisions")
	}
	known := make(map[string]struct{}, len(resources))
	values := make(map[string]bool, len(resources))
	for i, candidate := range resources {
		known[candidate.Ref.ID] = struct{}{}
		if authErr == nil {
			values[candidate.Ref.ID] = decisions[i]
		}
	}
	s.decisions, s.known, s.err = values, known, authErr
	close(ready)
	allowed, ok := values[resource.Ref.ID]
	s.mu.Unlock()
	if !ok {
		return false, true, authErr
	}
	return allowed, true, authErr
}
