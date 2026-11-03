package service

import (
	"context"
	"encoding/json"
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

type readBatchResourceKey struct {
	kind string
	ref  string
}

func readBatchKey(resource model.ResourceRef) (readBatchResourceKey, error) {
	ref, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &resource.Ref})
	return readBatchResourceKey{kind: resource.Kind, ref: ref}, err
}

// ReadBatchSession holds one finite set of resources and its decisions for a
// single view request. It is deliberately not a cache: no session is reused
// across calls, and an authority error is retained as an error for the rest of
// this request rather than becoming an absence.
type ReadBatchSession struct {
	mu           sync.Mutex
	resources    []model.ResourceRef
	members      map[readBatchResourceKey]struct{}
	principalKey string
	started      bool
	ready        chan struct{}
	decisions    map[readBatchResourceKey]bool
	finished     bool
	err          error
}

func WithReadBatchSession(ctx context.Context, resources []model.ResourceRef) (context.Context, *ReadBatchSession) {
	copyResources := append([]model.ResourceRef(nil), resources...)
	session := &ReadBatchSession{resources: copyResources, members: make(map[readBatchResourceKey]struct{}, len(resources))}
	for _, resource := range copyResources {
		key, err := readBatchKey(resource)
		if err != nil {
			session.err = err
			break
		}
		session.members[key] = struct{}{}
	}
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
	if err := ctx.Err(); err != nil {
		return false, true, err
	}
	// The idempotency PrincipalKey deliberately omits sponsor/caveats. A
	// decision session must instead bind the complete authorization principal.
	principal, err := json.Marshal(by)
	if err != nil {
		return false, true, err
	}
	principalKey := string(principal)
	key, err := readBatchKey(resource)
	if err != nil {
		return false, true, err
	}

	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return false, true, err
	}
	if _, declared := s.members[key]; !declared {
		s.mu.Unlock()
		return false, false, nil
	}
	if s.principalKey == "" {
		s.principalKey = principalKey
	} else if s.principalKey != principalKey {
		s.mu.Unlock()
		return false, true, errors.New("read authorization session principal changed")
	}
	if s.finished {
		allowed := s.decisions[key] && s.err == nil
		authErr := s.err
		s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return false, true, err
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
		allowed := s.decisions[key] && s.err == nil
		authErr := s.err
		s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return false, true, err
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
	if authErr == nil {
		authErr = ctx.Err()
	}
	values := make(map[readBatchResourceKey]bool, len(resources))
	for i, candidate := range resources {
		if authErr == nil {
			candidateKey, _ := readBatchKey(candidate) // validated at construction
			if previous, exists := values[candidateKey]; exists && previous != decisions[i] {
				authErr = errors.New("read authorization session returned inconsistent decisions")
				break
			}
			values[candidateKey] = decisions[i]
		}
	}
	s.decisions, s.finished, s.err = values, true, authErr
	close(ready)
	allowed := values[key] && authErr == nil
	s.mu.Unlock()
	return allowed, true, authErr
}
