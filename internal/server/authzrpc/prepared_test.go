package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Count the actual JSON normalization of the host's principal, rather than a
// new helper invocation. The finite actions value itself remains ordinary JSON.
type preparationActions struct{ calls *atomic.Int32 }

func (a preparationActions) MarshalJSON() ([]byte, error) {
	a.calls.Add(1)
	return json.Marshal([]string{"read"})
}

type preparationSnapshot struct{ observations int }

func (s *preparationSnapshot) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	return nil, errors.New("a User read filter must not load a project or record")
}

func (s *preparationSnapshot) Observe(_ context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	s.observations += len(keys)
	return make([]driver.LockState, len(keys)), nil
}

func preparationFixture(t *testing.T) (authz.Chain, authz.Caller, *atomic.Int32) {
	t.Helper()
	n := authz.Namespace{StoreID: "preparation", Incarnation: "one", InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	r, err := authzfixed.New(n)
	if err != nil {
		t.Fatal(err)
	}
	calls := new(atomic.Int32)
	caller := authz.Caller{
		Principal:      model.PrincipalRef{Kind: "user", ID: "reader", Caveats: map[string]any{"actions": preparationActions{calls}}},
		VerificationID: "verified-reader", CapturedEpoch: model.Natural64(1),
	}
	return authz.Chain{Namespace: n, Rules: []authz.RuleSet{r}}, caller, calls
}

func preparationSlice(chain authz.Chain, caller authz.Caller) (*slice, *preparationSnapshot) {
	snapshot := new(preparationSnapshot)
	return &slice{snapshot: snapshot, facts: authzfixed.Facts{Namespace: chain.Namespace}, expected: caller}, snapshot
}

// Exercise the same compiled Match and real authority slice used by read
// batches and filterPage, without changing the separately owned service code.
func TestPreparedReadBindingNormalizesHostContextOnce(t *testing.T) {
	chain, caller, calls := preparationFixture(t)
	template, err := chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.list"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s, snapshot := preparationSlice(chain, caller)
	calls.Store(0)
	for i := 0; i < 100; i++ {
		ok, err := template.Match(ctx, s, authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))})
		if err != nil || !ok {
			t.Fatalf("read %d: allowed=%v error=%v", i, ok, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("host principal normalized %d times for one immutable read binding; want 1", got)
	}
	if snapshot.observations != 4 {
		t.Errorf("common authority observations=%d; want the four tracked dependencies once", snapshot.observations)
	}
	// A later cancellation still fails even after successful common validation.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	ok, err := template.Match(canceled, s, authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))})
	var failure *authz.Error
	if ok || !errors.As(err, &failure) || failure.Code != "authz_unavailable" || !failure.Retryable {
		t.Fatalf("canceled binding: allowed=%v error=%v", ok, err)
	}
	// The same compiled parsing on a fresh retry/snapshot cannot retain success.
	changed := caller
	changed.CapturedEpoch = model.Natural64(2)
	fresh, freshSnapshot := preparationSlice(chain, changed)
	calls.Store(0)
	ok, err = template.Match(ctx, fresh, authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))})
	if err != nil || ok || calls.Load() != 1 || freshSnapshot.observations != 4 {
		t.Fatalf("fresh authority: allowed=%v error=%v normalizations=%d observations=%d", ok, err, calls.Load(), freshSnapshot.observations)
	}
}

func TestPreparedFixedBatchNormalizesEachBindingOnce(t *testing.T) {
	for _, throughChain := range []bool{false, true} {
		name := "fixed_eval"
		if throughChain {
			name = "chain_check"
		}
		t.Run(name, func(t *testing.T) {
			chain, caller, calls := preparationFixture(t)
			s, snapshot := preparationSlice(chain, caller)
			requests := make([]authz.CheckRequest, 100)
			for i := range requests {
				requests[i] = authz.CheckRequest{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))}, Method: "bead.get"}
			}
			var decisions []authz.Decision
			var err error
			if throughChain {
				decisions, err = chain.Check(context.Background(), s, requests)
			} else {
				decisions, err = chain.Rules[0].Eval(context.Background(), s, requests)
			}
			if err != nil || len(decisions) != len(requests) {
				t.Fatalf("decisions=%d error=%v", len(decisions), err)
			}
			for i, decision := range decisions {
				if decision.Effect != authz.Allow {
					t.Fatalf("decision %d: %+v", i, decision)
				}
			}
			// One freeze of the raw caller and one authoritative host comparison.
			if got := calls.Load(); got != 2 {
				t.Errorf("raw/host principal normalizations=%d; want 2 for one full binding", got)
			}
			if snapshot.observations != 4 {
				t.Errorf("common authority observations=%d; want 4", snapshot.observations)
			}
		})
	}
}

func TestPreparedContextPreservesPrincipalAndEpochEqualityDomains(t *testing.T) {
	chain, caller, _ := preparationFixture(t)
	for _, tc := range []struct {
		name, expected, supplied string
		want                     bool
	}{
		{"large_equal", "9007199254740993", "9007199254740993", true},
		{"large_distinct", "9007199254740993", "9007199254740992", false},
		{"lexical_decimal", "1", "1.0", false},
		{"lexical_exponent", "1", "1e0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected, supplied := caller, caller
			expected.Principal.Caveats = map[string]any{"opaque": json.Number(tc.expected)}
			supplied.Principal.Caveats = map[string]any{"opaque": json.Number(tc.supplied)}
			// Epoch equality is mathematical, unlike arbitrary caveat numbers.
			supplied.CapturedEpoch = model.Natural("1.0")
			prepared, err := authz.PrepareRequest(authz.ListRequest{Caller: supplied})
			if err != nil {
				t.Fatal(err)
			}
			s, _ := preparationSlice(chain, expected)
			got, err := s.CheckContext(context.Background(), prepared.Request().Caller)
			if err != nil || got != tc.want {
				t.Fatalf("prepared host comparison=%v/%v want=%v", got, err, tc.want)
			}
		})
	}
}

func TestPreparedBatchRebindsFreshAuthoritySnapshot(t *testing.T) {
	chain, caller, calls := preparationFixture(t)
	q := authz.CheckRequest{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))}, Method: "bead.get"}
	batch := authz.PrepareChecks([]authz.CheckRequest{q})
	binding, err := batch.Binding(0)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := chain.Rules[0].(authz.PreparedCompiler).CompilePrepared(binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, useEval := range []bool{false, true} {
		name := "batch_match"
		if useEval {
			name = "fixed_eval_prepared"
		}
		t.Run(name, func(t *testing.T) {
			for i, epoch := range []model.Revision{model.Natural64(1), model.Natural64(2), model.Natural64(1)} {
				expected := caller
				expected.CapturedEpoch = epoch
				s, snapshot := preparationSlice(chain, expected)
				calls.Store(0)
				var allowed bool
				var err error
				if useEval {
					var decisions []authz.Decision
					decisions, err = chain.Rules[0].(authz.PreparedRuleSet).EvalPrepared(context.Background(), s, batch)
					allowed = len(decisions) == 1 && decisions[0].Effect == authz.Allow
				} else {
					allowed, err = batch.Match(context.Background(), s, 0, filter)
				}
				if err != nil || allowed != (i != 1) || calls.Load() != 1 || snapshot.observations != 4 {
					t.Fatalf("snapshot %d: allowed=%v error=%v host normalizations=%d observations=%d", i, allowed, err, calls.Load(), snapshot.observations)
				}
			}
		})
	}
}
