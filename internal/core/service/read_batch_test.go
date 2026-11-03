package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func batchResource(id string) model.ResourceRef {
	return model.ResourceRef{Kind: "bead", Ref: model.RefByID(id, model.Natural64(0))}
}

func TestReadBatchBindsFullPrincipalAndResourceIdentity(t *testing.T) {
	a, b := batchResource("batch-a"), batchResource("batch-b")
	by := model.PrincipalRef{Kind: "user", ID: "batch-reader"}
	ctx, session := WithReadBatchSession(context.Background(), []model.ResourceRef{a, b})
	calls := 0
	authorize := func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error) {
		calls++
		return []bool{true, false}, nil
	}
	if ReadBatchFromContext(ctx) != session || ReadBatchFromContext(context.Background()) != nil {
		t.Fatal("batch session escaped its request context")
	}
	for _, tc := range []struct {
		resource model.ResourceRef
		allowed  bool
		handled  bool
	}{
		{a, true, true},
		{b, false, true},
		{model.ResourceRef{Kind: "memory", Ref: a.Ref}, false, false},
		{model.ResourceRef{Kind: "bead", Ref: model.RefByID(a.Ref.ID, model.Natural64(1))}, false, false},
		{model.ResourceRef{Kind: "bead", Ref: model.RefByKey(a.Ref.ID)}, false, false},
		{batchResource("outside"), false, false},
	} {
		allowed, handled, err := session.Resolve(ctx, by, authorize, tc.resource)
		if err != nil || allowed != tc.allowed || handled != tc.handled {
			t.Fatalf("resource decision=%t,%t,%v for %+v", allowed, handled, err, tc.resource)
		}
	}
	if calls != 1 {
		t.Fatalf("one request repeated batch authorization %d times", calls)
	}
	narrow := by
	narrow.Caveats = map[string]any{"refs": []model.Ref{b.Ref}}
	if allowed, handled, err := session.Resolve(ctx, narrow, authorize, a); err == nil || allowed || !handled {
		t.Fatalf("same identity reused broader caveat decisions: %t,%t,%v", allowed, handled, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if allowed, handled, err := session.Resolve(canceled, by, authorize, a); !errors.Is(err, context.Canceled) || allowed || !handled {
		t.Fatalf("completed batch hid cancellation: %t,%t,%v", allowed, handled, err)
	}
	_, next := WithReadBatchSession(context.Background(), []model.ResourceRef{a, b})
	if _, _, err := next.Resolve(context.Background(), by, authorize, a); err != nil || calls != 2 {
		t.Fatalf("next request reused an earlier decision: calls=%d err=%v", calls, err)
	}
}

func TestReadBatchFailureNeverBecomesAnAllowedOrMissingDecision(t *testing.T) {
	a := batchResource("batch-a")
	by := model.PrincipalRef{Kind: "user", ID: "batch-reader"}
	for _, tc := range []struct {
		name      string
		resources []model.ResourceRef
		decisions []bool
		err       error
	}{
		{"source_error", []model.ResourceRef{a}, []bool{true}, errors.New("authority unavailable")},
		{"short_decisions", []model.ResourceRef{a}, nil, nil},
		{"inconsistent_duplicates", []model.ResourceRef{a, a}, []bool{true, false}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, session := WithReadBatchSession(context.Background(), tc.resources)
			calls := 0
			authorize := func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error) {
				calls++
				return tc.decisions, tc.err
			}
			for range 2 {
				allowed, handled, err := session.Resolve(ctx, by, authorize, a)
				if err == nil || allowed || !handled || (tc.err != nil && !errors.Is(err, tc.err)) {
					t.Fatalf("authority error was hidden: %t,%t,%v", allowed, handled, err)
				}
			}
			if calls != 1 {
				t.Fatalf("failed batch retried %d times in one request", calls)
			}
		})
	}
}

func TestReadBatchBindsSponsorIdentity(t *testing.T) {
	a := batchResource("batch-a")
	by := model.PrincipalRef{Kind: "agent", ID: "batch-agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "first-sponsor"}}
	ctx, session := WithReadBatchSession(context.Background(), []model.ResourceRef{a})
	called := 0
	authorize := func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error) {
		called++
		return []bool{true}, nil
	}
	if allowed, handled, err := session.Resolve(ctx, by, authorize, a); err != nil || !allowed || !handled {
		t.Fatalf("first sponsor decision=%t,%t,%v", allowed, handled, err)
	}
	by.Sponsor = &model.PrincipalRef{Kind: "user", ID: "second-sponsor"}
	if allowed, handled, err := session.Resolve(ctx, by, authorize, a); err == nil || allowed || !handled || called != 1 {
		t.Fatalf("changed sponsor reused prior authority: %t,%t,%v calls=%d", allowed, handled, err, called)
	}
}

func TestReadBatchConcurrentResolutionRunsOneAuthorizer(t *testing.T) {
	a := batchResource("batch-a")
	by := model.PrincipalRef{Kind: "user", ID: "batch-reader"}
	ctx, session := WithReadBatchSession(context.Background(), []model.ResourceRef{a})
	var calls atomic.Int32
	entered, release := make(chan struct{}, 8), make(chan struct{})
	authorize := func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return []bool{true}, nil
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			allowed, handled, err := session.Resolve(ctx, by, authorize, a)
			if err != nil || !allowed || !handled {
				t.Errorf("concurrent batch decision=%t,%t,%v", allowed, handled, err)
			}
		}()
	}
	<-entered
	close(release)
	group.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent request invoked %d authorizers", calls.Load())
	}
}
