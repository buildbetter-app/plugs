package authzrpc

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReadBatchGetRetainsPayloadProjectionAndFreshAuthority(t *testing.T) {
	f := newHFixture(t, true)
	by := model.PrincipalRef{Kind: "user", ID: "user:reader", Caveats: map[string]any{"refs": []model.Ref{hRef("visible")}}}
	ctx, _ := service.WithReadBatchSession(context.Background(), []model.ResourceRef{{Kind: "bead", Ref: hRef("visible")}})
	for range 2 {
		got, err := f.app.Service.Get(ctx, by, service.GetRequest{ID: hRef("visible")})
		if err != nil || got.Bead.ID != "visible" || got.Bead.Type != "issue" || got.Bead.Fields["title"] != "visible" || !got.Bead.ParentHidden || got.Bead.Parent != "" {
			t.Fatalf("batched Get lost payload or exposed its denied parent: %+v, %v", got, err)
		}
	}
	// A decision retained in this request cannot bypass the normal current
	// authority recheck after a custom-policy change.
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"visible": true}, action: authz.Read}}
	if got, err := f.app.Service.Get(ctx, by, service.GetRequest{ID: hRef("visible")}); err == nil || got.Bead.ID != "" {
		t.Fatalf("cached batch decision escaped the final current-authority fence: %+v, %v", got, err)
	}
}

func TestFastUserReadPageMatchesReferenceAndFallsBack(t *testing.T) {
	f := newHFixture(t, true)
	items := []service.Bead{{ID: "visible"}, {ID: "new"}, {ID: ""}, {ID: "invalid/slash"}, {ID: " padded"}}
	for _, tc := range []struct {
		name    string
		caveats map[string]any
		fast    bool
	}{
		{"unrestricted", nil, true},
		{"refs", map[string]any{"refs": []model.Ref{hRef("visible")}}, true},
		{"empty_refs", map[string]any{"refs": []model.Ref{}}, true},
		{"denied_action", map[string]any{"actions": []string{"modify"}}, true},
		{"projects", map[string]any{"projects": []model.Ref{hRef("child-project")}}, false},
		{"malformed_refs", map[string]any{"refs": "invalid"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			by := model.PrincipalRef{Kind: "user", ID: "user:reader", Caveats: tc.caveats}
			caller, err := f.app.Checker.callerFor(context.Background(), by)
			if err != nil {
				t.Fatal(err)
			}
			facts, err := f.app.Checker.load(context.Background(), f.snapshot(t), caller)
			if err != nil {
				t.Fatal(err)
			}
			template, err := f.app.Checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.ready"})
			if err != nil {
				t.Fatal(err)
			}
			got, handled, err := fastUserReadPage(context.Background(), caller, facts, template, items)
			if err != nil || handled != tc.fast {
				t.Fatalf("fast path handled=%t err=%v", handled, err)
			}
			if !handled {
				return
			}
			want := make([]service.Bead, 0)
			for _, item := range items {
				allowed, err := template.Match(context.Background(), facts, authz.Resource{Kind: authz.Bead, Ref: hRef(item.ID)})
				if err != nil {
					t.Fatal(err)
				}
				if allowed {
					want = append(want, item)
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fast permission decision=%+v reference=%+v", got, want)
			}
		})
	}
}

// This snapshot makes dependency observations without consulting context so
// the test cancels specifically during the per-item loop.
type rewriteObservationSnapshot struct{}

func (rewriteObservationSnapshot) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	return nil, errors.New("unconditional User read unexpectedly requested a record")
}

func (rewriteObservationSnapshot) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, nil
}

type cancelSecondItemContext struct {
	context.Context
	cancel context.CancelFunc
	calls  int
}

func (c *cancelSecondItemContext) Err() error {
	c.calls++
	if c.calls == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestFastUserReadPageCancellationDiscardsPartialItems(t *testing.T) {
	f := newHFixture(t, true)
	by := model.PrincipalRef{Kind: "user", ID: "user:reader"}
	caller, err := f.app.Checker.callerFor(context.Background(), by)
	if err != nil {
		t.Fatal(err)
	}
	template, err := f.app.Checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.ready"})
	if err != nil {
		t.Fatal(err)
	}
	facts := &slice{snapshot: rewriteObservationSnapshot{}, facts: f.base, expected: caller}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controlled := &cancelSecondItemContext{Context: ctx, cancel: cancel}
	items, handled, err := fastUserReadPage(controlled, caller, facts, template, []service.Bead{{ID: "visible"}, {ID: "new"}})
	if !handled || !errors.Is(err, context.Canceled) || items != nil || controlled.calls != 2 {
		t.Fatalf("cancellation published partial/complete items: %+v handled=%t calls=%d err=%v", items, handled, controlled.calls, err)
	}
}
