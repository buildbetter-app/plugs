package cp7latency_test

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestRealCP7MembershipCountsParentsAndCurrentAuthority(t *testing.T) {
	checkDeclaration(t)
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 1000)
			ctx := context.Background()
			for _, divisor := range []int{1, 100, 1000} {
				t.Run(map[int]string{1: "UserAllRead", 100: "Refs1pct", 1000: "Refs0.1pct"}[divisor], func(t *testing.T) {
					p := f.user(divisor)
					page, err := f.app.Service.Ready(ctx, p, service.ReadyRequest{Project: projectName, Limit: pageSize})
					if err != nil {
						t.Fatal(err)
					}
					f.checkPage(t, page, divisor)
					// Traverse every page and independently enumerate the exact set.
					all := append([]service.Bead(nil), page.Items...)
					for page.Next != nil {
						page, err = f.app.Service.Ready(ctx, p, service.ReadyRequest{Project: projectName, Limit: pageSize, Position: page.Next})
						if err != nil {
							t.Fatal(err)
						}
						all = append(all, page.Items...)
						if len(all) > f.n {
							t.Fatal("non-terminating/duplicate page walk")
						}
					}
					if len(all) != f.n/divisor {
						t.Fatal("page walk lost the authorized tail")
					}
					for i, row := range all {
						if row.ID != beadRef((i+1)*divisor).ID {
							t.Fatal("compiled list membership differs from independent corpus oracle")
						}
					}
					grouped, err := f.views.Run(ctx, p, groupedRequest())
					if err != nil {
						t.Fatal(err)
					}
					f.checkGrouped(t, grouped, divisor)
					// Point permissions must agree, including a denied neighbor.
					if _, err := f.app.Service.Get(ctx, p, service.GetRequest{ID: beadRef(divisor)}); err != nil {
						t.Fatal(err)
					}
					if divisor > 1 {
						_, err := f.app.Service.Get(ctx, p, service.GetRequest{ID: beadRef(divisor - 1)})
						requireCode(t, err, "not_found")
					}
				})
			}
			t.Run("AgentDoesNotBorrowUserRead", func(t *testing.T) {
				_, err := f.app.Service.Get(ctx, f.agent(100, true), service.GetRequest{ID: beadRef(100)})
				requireCode(t, err, "not_found")
				page, err := f.app.Service.Ready(ctx, f.agent(100, true), service.ReadyRequest{Project: projectName, Limit: pageSize})
				if err != nil || len(page.Items) != 0 || page.Next != nil {
					t.Fatalf("Agent borrowed its sponsor's User read: %v", err)
				}
			})
			t.Run("DirectAndOneHopFullClaimReceipt", func(t *testing.T) {
				for i, team := range []bool{false, true} {
					p := f.agent(100, team)
					meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("correctness", 100, "claim", i)}
					out, err := f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
					if err != nil {
						t.Fatal(err)
					}
					f.checkClaim(t, p, out, meta, 100)
					f.resetClaim(t, out.Commit.Results[0].Ref)
				}
			})
			t.Run("RevocationInvalidatesCursorAndClaimReplay", func(t *testing.T) {
				p := f.agent(100, false)
				meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("correctness", 100, "revoked", 0)}
				out, err := f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
				if err != nil {
					t.Fatal(err)
				}
				f.checkClaim(t, p, out, meta, 100)
				f.resetClaim(t, out.Commit.Results[0].Ref)
				first, err := f.app.Service.Ready(ctx, f.user(1), service.ReadyRequest{Project: projectName, Limit: pageSize})
				if err != nil || first.Next == nil {
					t.Fatalf("cursor control: %v", err)
				}
				next := f.base
				next.Grants = nil
				if err := f.facts.Replace(next); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := f.facts.Replace(f.base); err != nil {
						t.Error(err)
					}
				}()
				_, err = f.app.Service.Ready(ctx, f.user(1), service.ReadyRequest{Project: projectName, Limit: pageSize, Position: first.Next})
				requireCode(t, err, "cursor_invalid")
				_, err = f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
				requireCode(t, err, "forbidden")
				// Removing involvement never removes CP7's independent User read.
				if _, err := f.app.Service.Get(ctx, f.user(1), service.GetRequest{ID: beadRef(100)}); err != nil {
					t.Fatal(err)
				}
				// The existing Checker also requires the project permit for a
				// terminal receipt. Revocation must publish no fresh receipt.
				meta.IdempotencyKey = requestKey("correctness", 100, "terminal", 0)
				before, err := f.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				empty, err := f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
				requireCode(t, err, "forbidden")
				after, err := f.store.Meta(ctx)
				if err != nil || empty != nil || before.Head.String() != after.Head.String() {
					t.Fatalf("revoked terminal request published state: %v", err)
				}
				principal, err := driver.PrincipalKey(p)
				if err != nil {
					t.Fatal(err)
				}
				r := f.reader(t, nil)
				defer r.Close()
				receipt, err := r.IdempotencyByKey(ctx, principal, "", "bead.claimNext", meta.IdempotencyKey)
				if err != nil || receipt != nil {
					t.Fatalf("revoked terminal request minted a receipt: %v", err)
				}
			})
			t.Run("AuthorizedExhaustedSelectorCommitsTerminalReceipt", func(t *testing.T) {
				p := f.agent(1, false)
				req := service.ClaimNextRequest{Project: projectName, Filter: &service.Filter{Resources: []string{beadRef(100).ID}}}
				meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("correctness", 1, "drain", 0)}
				claimed, err := f.app.Service.ClaimNextWithOutcome(ctx, p, req, meta)
				if err != nil || claimed == nil || claimed.NullResult || len(claimed.Commit.Results) != 1 || claimed.Commit.Results[0].Ref.ID != beadRef(100).ID {
					t.Fatalf("authorized selector control did not claim its one bead: %v", err)
				}
				defer f.resetClaim(t, beadRef(100))
				before, err := f.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				meta.IdempotencyKey = requestKey("correctness", 1, "empty", 0)
				empty, err := f.app.Service.ClaimNextWithOutcome(ctx, p, req, meta)
				if err != nil || empty == nil || !empty.NullResult || empty.Replayed || len(empty.Commit.Results) != 0 {
					t.Fatalf("authorized terminal outcome: %v", err)
				}
				after, err := f.store.Meta(ctx)
				beforeRevision, numberErr := before.Head.Uint64()
				if err != nil || numberErr != nil || after.Head.String() != model.Natural64(beforeRevision+1).String() || empty.Commit.Revision.String() != after.Head.String() {
					t.Fatal("authorized terminal receipt did not publish exactly H+1")
				}
				principal, err := driver.PrincipalKey(p)
				if err != nil {
					t.Fatal(err)
				}
				r := f.reader(t, nil)
				receipt, err := r.IdempotencyByKey(ctx, principal, "", "bead.claimNext", meta.IdempotencyKey)
				r.Close()
				if err != nil || receipt == nil || receipt.Commit.Revision.String() != after.Head.String() || len(receipt.Changeset.Changes) != 0 || len(receipt.Changeset.Events) != 0 {
					t.Fatalf("terminal receipt is not durable zero-change state: %v", err)
				}
				replayed, err := f.app.Service.ClaimNextWithOutcome(ctx, p, req, meta)
				if err != nil || replayed == nil || !replayed.Replayed || !replayed.NullResult || replayed.Commit.Revision.String() != after.Head.String() {
					t.Fatalf("terminal receipt replay drifted: %v", err)
				}
			})
			t.Run("PinnedContentUsesCurrentPrincipalAndCompleteQuery", func(t *testing.T) {
				p := f.user(100)
				page, err := f.app.Service.History(ctx, p, service.HistoryRequest{Project: projectName, AsOf: f.seededAt, Limit: pageSize})
				if err != nil {
					t.Fatal(err)
				}
				f.checkPage(t, page, 100)
				if page.AsOf.Revision.String() != f.seededAt.Revision.String() {
					t.Fatal("history silently read current content")
				}
				p.Caveats = map[string]any{"refs": []model.Ref{}}
				page, err = f.app.Service.History(ctx, p, service.HistoryRequest{Project: projectName, AsOf: f.seededAt, Limit: pageSize})
				if err != nil || len(page.Items) != 0 {
					t.Fatalf("pinned content borrowed old visibility: %v", err)
				}
				request := groupedRequest()
				request.Definition.Filter.Query = "title=cp7 AND type=issue"
				got, err := f.views.Run(ctx, f.user(1), request)
				if err != nil || len(got.Groups) != 0 || len(got.Items) != 0 {
					t.Fatalf("case-sensitive title predicate changed: %v", err)
				}
				request.Definition.Filter.Query = "type=issue OR title=CP7"
				_, err = f.views.Run(ctx, f.user(1), request)
				requireCode(t, err, "invalid_params")
			})
		})
	}
}

func requireCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != code {
		t.Fatalf("error=%v want=%s", err, code)
	}
}

func TestMeasurementRejectsRefusalAsLatencySuccess(t *testing.T) {
	observations := make([]sample, measuredSamples)
	for i := range observations {
		observations[i] = sample{Engine: "sqlite", FractionDivisor: 1, Operation: "Q4Ready", Phase: "measured", Index: i, Corpus: corpusSize, ExpectedAuthorized: corpusSize, Status: "success", ElapsedMS: 1}
	}
	result, err := summarize("sqlite", 1, "Q4Ready", "test", sample{}, observations)
	if err != nil || !result.TargetPass || result.SuccessfulP95MS == nil {
		t.Fatal("successful control was not measured")
	}
	observations[0].Status = "cap_refusal"
	result, err = summarize("sqlite", 1, "Q4Ready", "test", sample{}, observations)
	if err != nil || result.TargetPass || result.SuccessfulP95MS != nil || result.Status != "RED" {
		t.Fatal("a fast refusal falsely passed product latency")
	}
	observations[0].Status = "request_deadline"
	result, err = summarize("sqlite", 1, "Q4Ready", "test", sample{}, observations)
	if err != nil || result.TargetPass || result.SuccessfulP95MS != nil {
		t.Fatal("deadline was treated as latency success")
	}
	observations[0].Status = "correctness_error"
	if _, err := summarize("sqlite", 1, "Q4Ready", "test", sample{}, observations); err == nil {
		t.Fatal("correctness error accepted")
	}
	if _, err := summarize("sqlite", 1, "Q4Ready", "test", sample{}, observations[:19]); err == nil {
		t.Fatal("nineteen samples accepted")
	}
}

func TestIndependentOracleRejectsMembershipParentAndPartialCount(t *testing.T) {
	page := service.BeadPage{Items: []service.Bead{{ID: beadRef(1000).ID, ParentHidden: true}}}
	if err := validatePage(page, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	page.Items[0].Parent = beadRef(999).ID
	if err := validatePage(page, 1000, 1000); err == nil {
		t.Fatal("oracle accepted a hidden parent identity")
	}
	page.Items[0] = service.Bead{ID: beadRef(999).ID, ParentHidden: true}
	if err := validatePage(page, 1000, 1000); err == nil {
		t.Fatal("oracle accepted unauthorized membership")
	}
	got := viewops.Result{Groups: []viewops.Group{{Key: "issue", Count: 1, Items: []viewops.Bead{{ID: beadRef(1000).ID, Type: "issue", Fields: map[string]any{"title": "CP7 bead 001000"}, ParentHidden: true}}}}}
	if err := validateGrouped(got, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	got.Groups[0].Count = 0
	if err := validateGrouped(got, 1000, 1000); err == nil {
		t.Fatal("oracle accepted partial/wrong count")
	}
	got.Groups[0].Count = 1
	got.Groups[0].Items[0].Parent = beadRef(999).ID
	if err := validateGrouped(got, 1000, 1000); err == nil {
		t.Fatal("grouped oracle accepted a hidden parent identity")
	}
}
