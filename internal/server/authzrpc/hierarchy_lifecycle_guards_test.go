package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func hierarchyLifecycleNoReceipt(t *testing.T, f *hFixture, params map[string]any) {
	t.Helper()
	principal, err := driver.PrincipalKey(hAgent())
	if err != nil {
		t.Fatal(err)
	}
	key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
	receipt, err := f.snapshot(t).IdempotencyByKey(context.Background(), principal, "", "tx.apply", key)
	if err != nil || receipt != nil {
		t.Fatalf("refused hierarchy lifecycle receipt=%+v err=%v", receipt, err)
	}
}

func TestHierarchyMixedLifecycleIndependentPermitsAndLateGuardRollback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, refusal := range []string{"tx.apply", "bead.close", "late-template"} {
			t.Run(engine+"/"+refusal, func(t *testing.T) {
				f := newHFixtureStore(t, true, hierarchyLifecycleStore(t, engine))
				rules := &hierarchyLifecycleRules{denyTx: refusal == "tx.apply", denyClose: refusal == "bead.close"}
				f.app.Checker.chain.Rules = []authz.RuleSet{rules}
				params := hierarchyLifecycleMixedParams(2100, nil)
				want := model.ErrorCode("forbidden")
				if refusal == "late-template" {
					ops := params["changeSet"].(map[string]any)["ops"].([]any)
					ops[0].(map[string]any)["fields"].(map[string]any)["template"] = true
					want = "ontology_guard_failed"
				}
				head, rows := hierarchyLifecycleState(t, f, hRef("child"))
				count := hierarchyLifecycleCount(t, f)
				reply := f.callRPC(t, hAgent(), "tx.apply", params)
				if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 || reply.Meta.Replayed {
					t.Fatalf("refusal=%+v want=%s", reply, want)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				if hierarchyLifecycleCount(t, f) != count {
					t.Fatal("late refusal published the staged create")
				}
				hierarchyLifecycleNoReceipt(t, f, params)
			})
		}
	}
}

// This installed test policy explicitly permits both methods using current
// membership facts. Production's finite CP7 policy remains unchanged and is
// separately tested below to grant neither tx.apply nor bead.close by default.
type hierarchyLifecycleMembership struct{ fixed *authzfixed.RuleSet }

func (r hierarchyLifecycleMembership) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	mapped := append([]authz.CheckRequest(nil), requests...)
	for i := range mapped {
		if mapped[i].Method == "tx.apply" || mapped[i].Method == "bead.close" {
			mapped[i].Method = "bead.update"
			if mapped[i].Resource.Kind == authz.Project {
				mapped[i].Method = ""
			}
		}
	}
	return r.fixed.Eval(ctx, s, mapped)
}
func (hierarchyLifecycleMembership) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func TestHierarchyMixedCloseRechecksCurrentMembershipAndPreservesFinitePolicy(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHFixtureStore(t, true, hierarchyLifecycleStore(t, engine))
			params := hierarchyLifecycleMixedParams(2110, nil)
			head, rows := hierarchyLifecycleState(t, f, hRef("child"))
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			hierarchyLifecycleNoReceipt(t, f, params)
			fixed, err := authzfixed.New(f.base.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			f.app.Checker.chain.Rules = []authz.RuleSet{hierarchyLifecycleMembership{fixed}}
			first := f.callRPC(t, hAgent(), "tx.apply", params)
			if first.Error != nil || first.Meta.Replayed {
				t.Fatalf("configured membership grant failed: %+v", first)
			}
			for _, project := range []string{"child-project", "parent-project"} {
				facts := f.base
				facts.Grants = nil
				for _, grant := range f.base.Grants {
					if grant.Scope.Ref.ID != project {
						facts.Grants = append(facts.Grants, grant)
					}
				}
				if err := f.facts.Replace(facts); err != nil {
					t.Fatal(err)
				}
				// Replace records its own authority revision; measure the replay
				// against the state AFTER that legitimate fact mutation.
				head, rows = hierarchyLifecycleState(t, f, hRef("child"))
				hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				if err := f.facts.Replace(f.base); err != nil {
					t.Fatal(err)
				}
				head, rows = hierarchyLifecycleState(t, f, hRef("child"))
				replay := f.callRPC(t, hAgent(), "tx.apply", params)
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
					t.Fatalf("restored current %s authority replay=%+v", project, replay)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			}
		})
	}
}

func TestHierarchyMixedCloseExpectationOnlyPrivacyAndReparentedTarget(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHFixtureStore(t, true, hierarchyLifecycleStore(t, engine))
			rules := &hierarchyLifecycleRules{}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			params := hierarchyLifecycleMixedParams(2120, nil)
			ops := params["changeSet"].(map[string]any)["ops"].([]any)
			close := ops[2].(map[string]any)
			close["target"] = hRef("child") // same stored target reparented in slot 1.
			hidden := hRef("hidden-one")
			close["expect"] = []model.Precondition{model.PreconditionByRevision(hidden, model.Natural64(0))}
			rules.setRef(hidden.ID, true)
			head, rows := hierarchyLifecycleState(t, f, hRef("child"))
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			hierarchyLifecycleNoReceipt(t, f, params)
			rules.setRef(hidden.ID, false)
			stale := f.callRPC(t, hAgent(), "tx.apply", params)
			if stale.Error == nil || stale.Error.Data.Code != "precondition_failed" || stale.Error.Data.Details["reason"] != "revision" || fmt.Sprint(stale.Error.Data.Details["currentRevision"]) != hierarchyLifecycleRevision(t, f, hidden).String() {
				t.Fatalf("allowed stale close expectation=%+v", stale)
			}
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			hierarchyLifecycleNoReceipt(t, f, params)
			close["expect"] = []model.Precondition{model.PreconditionByRevision(hidden, hierarchyLifecycleRevision(t, f, hidden))}
			first := f.callRPC(t, hAgent(), "tx.apply", params)
			if first.Error != nil || first.Meta.Replayed {
				t.Fatalf("reparent then close failed: %+v", first)
			}
			var commit model.Commit
			if err := json.Unmarshal(first.Result, &commit); err != nil || len(commit.Results) != 3 || commit.Results[1].Ref.ID != "child" || commit.Results[2].Ref.ID != "child" {
				t.Fatalf("mixed result=%s err=%v", first.Result, err)
			}
			head, rows = hierarchyLifecycleState(t, f, hRef("child"))
			rules.setRef(hidden.ID, true)
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			rules.setRef(hidden.ID, false)
			replay := f.callRPC(t, hAgent(), "tx.apply", params)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("restored close expectation replay=%+v", replay)
			}
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
		})
	}
}

func TestHierarchyLifecycleFinalImagesRemainBoundToHierarchyChecks(t *testing.T) {
	f := newHFixture(t, true)
	f.app.Checker.chain.Rules = []authz.RuleSet{&hierarchyLifecycleRules{}}
	parent := hRef("new")
	move := hMove(&parent)
	close := port.Change{Resource: move.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: hRef("child")}}, Before: move.Before, After: move.After}
	for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter} {
		proposal := hProposal(hAgent(), "tx.apply", move, close)
		if err := f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t)); err != nil {
			t.Fatalf("valid reparent/close images: %v", err)
		}
		for _, forged := range []string{"old", "hidden-one"} {
			proposal.Changes[1].After = hBody("child", "child-project", forged, "todo")
			err := f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t))
			requireHCode(t, err, "forbidden")
		}
	}
	create := hCreate("fresh", "fresh", "child-project", nil, "")
	close = port.Change{Resource: create.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: model.RefByKey("fresh")}}, After: create.After}
	proposal := hProposal(hAgent(), "tx.apply", create, close)
	if err := f.app.Checker.Authorize(context.Background(), port.AuthorizeAfter, proposal, f.snapshot(t)); err != nil {
		t.Fatalf("valid staged create/close images: %v", err)
	}
	for _, field := range []string{"labels", "owner", "assignee", "restricted", "unexpected"} {
		var body map[string]any
		if err := json.Unmarshal(create.After, &body); err != nil {
			t.Fatal(err)
		}
		body[field] = "forged"
		bad, _ := json.Marshal(body)
		proposal.Changes[0].After, proposal.Changes[1].After = bad, bad
		requireHCode(t, f.app.Checker.Authorize(context.Background(), port.AuthorizeAfter, proposal, f.snapshot(t)), "forbidden")
	}
	if !reflect.DeepEqual(create.After, close.After) {
		t.Fatal("authorization mutated the caller's image")
	}
}
