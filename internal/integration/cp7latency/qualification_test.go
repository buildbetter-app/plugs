package cp7latency_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type qualificationReadiness func(context.Context, driver.Reader, []model.Ref) (map[string]viewops.ReadinessState, error)

func (fn qualificationReadiness) Pending(ctx context.Context, r driver.Reader, refs []model.Ref) (map[string]viewops.ReadinessState, error) {
	return fn(ctx, r, refs)
}

func TestGroupedEveryPageMatchesIndependentCorpus(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 1000)
			cfg := viewops.Config{
				OntologyRevision: model.Natural64(1),
				AuthorizeRead: func(ctx context.Context, p model.PrincipalRef, ref model.ResourceRef, _ string) error {
					_, err := f.app.Service.Get(ctx, p, service.GetRequest{ID: ref.Ref})
					return err
				},
				AuthorizeReadBatch: f.app.Service.AuthorizeReadBatch,
				ParentProjection: &viewops.ParentProjectionConfig{
					Project: func(ctx context.Context, p model.PrincipalRef, r driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
						a, err := f.app.Checker.AuthorizeParentProjections(ctx, p, r, refs)
						out := viewops.ParentProjectionAuthority{PolicyBinding: a.PolicyBinding, Parents: make(map[string]viewops.ParentDisclosure, len(a.Parents))}
						for id, value := range a.Parents {
							out.Parents[id] = viewops.ParentDisclosure{Exists: value.Exists, Ref: value.Ref}
						}
						return out, err
					},
					ValidateAuthority: f.app.Checker.ValidateReadAuthorityBinding,
				},
			}
			views, err := viewops.New(f.store.Store, f.lease, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, divisor := range []int{1, 100, 1000} {
				t.Run(fmt.Sprintf("divisor%d", divisor), func(t *testing.T) {
					req := groupedRequest()
					seen := map[string]bool{}
					position, total := 0, f.n/divisor
					var pinned viewops.AsOf
					for {
						var work counters
						got, err := views.Run(driver.WithReadWorkObserver(context.Background(), &work), f.user(divisor), req)
						if err != nil {
							t.Fatal(err)
						}
						if got.Count != nil || len(got.Items) != 0 || len(got.Groups) != 1 || got.Groups[0].Key != "issue" || got.Groups[0].Count != total {
							t.Fatalf("full group count changed on page at %d: %+v", position, got)
						}
						if work.value().Queries == 0 || work.work().Payloads == 0 {
							t.Fatalf("page did not execute real reader work: %+v %+v", work.work(), work.value())
						}
						if position == 0 {
							pinned = got.AsOf
						} else if !reflect.DeepEqual(pinned, got.AsOf) {
							t.Fatal("page content snapshot changed")
						}
						group := got.Groups[0]
						end := min(position+viewops.DefaultOutputBudget, total)
						if len(group.Items) != end-position {
							t.Fatal("page skipped or duplicated payloads")
						}
						for _, row := range group.Items {
							position++
							n := position * divisor
							doc := document(n)
							if row.ID != doc.ID || seen[row.ID] || row.Type != doc.Type || row.Fields["title"] != doc.Fields["title"] {
								t.Fatalf("global membership/order/payload differs at %d: %+v", position, row)
							}
							seen[row.ID] = true
							if doc.Parent == nil {
								if row.Parent != "" || row.ParentHidden {
									t.Fatal("root invented parent")
								}
							} else if divisor == 1 {
								if row.Parent != doc.Parent.ID || row.ParentHidden {
									t.Fatal("readable parent missing on deep page")
								}
							} else if row.Parent != "" || !row.ParentHidden {
								t.Fatal("hidden parent identity disclosed on deep page")
							}
						}
						if position != end {
							t.Fatal("page position drift")
						}
						if end == total {
							if got.NextCursor != nil || group.Elided != nil {
								t.Fatal("terminal page invented cursor or suffix")
							}
							break
						}
						if got.NextCursor == nil || got.NextCursor.Index != uint64(end) || group.Elided == nil || group.Elided.Count != total-end || group.Elided.ContinueIndex != uint64(end) {
							t.Fatal("cursor/suffix elision differs from complete authorized tail")
						}
						req.Cursor = got.NextCursor
					}
					if len(seen) != total {
						t.Fatal("all-page membership incomplete")
					}
					t.Logf("CP7_ALL_GROUPED_PAGES engine=%s divisor=%d unique=%d ordered=true complete_counts=true suffix_elisions=true parents=true", engine, divisor, len(seen))
				})
			}
		})
	}
}

func TestRefusedViewCountIsPartialOutputIncludingZero(t *testing.T) {
	for _, count := range []int{0, 1, 20001} {
		fault := viewops.Result{Count: &count}
		if !refusedOutput(service.BeadPage{}, nil, fault) {
			t.Fatalf("refusal with count %d was accepted as an empty response", count)
		}
	}
	if refusedOutput(service.BeadPage{}, nil, viewops.Result{}) {
		t.Fatal("empty refusal control rejected")
	}
	if !refusedOutput(service.BeadPage{}, &port.WriteOutcome{}, viewops.Result{}) {
		t.Fatal("refused write outcome accepted")
	}
}

func TestP95RequiresTwentyCompleteFiniteObservations(t *testing.T) {
	base := make([]sample, measuredSamples)
	for i := range base {
		base[i] = sample{Engine: "sqlite", FractionDivisor: 100, Corpus: corpusSize, ExpectedAuthorized: corpusSize / 100, Operation: "Q6GroupedCount", Phase: "measured", Index: i, Status: "success", ElapsedMS: 249}
	}
	good, err := summarize("sqlite", 100, "Q6GroupedCount", "test", sample{}, base)
	if err != nil || !good.TargetPass || good.TargetMS != 250 || good.SuccessfulP95MS == nil {
		t.Fatalf("complete summary control: %+v %v", good, err)
	}
	for _, status := range []string{"cap_refusal", "request_deadline", "write_pre_deadline"} {
		x := append([]sample(nil), base...)
		x[19].Status = status
		got, err := summarize("sqlite", 100, "Q6GroupedCount", "test", sample{}, x)
		if err != nil || got.TargetPass || got.SuccessfulP95MS != nil || got.Status != "RED" {
			t.Fatalf("incomplete successful summary qualified: %+v %v", got, err)
		}
	}
	for _, elapsed := range []float64{-1, math.NaN(), math.Inf(1)} {
		x := append([]sample(nil), base...)
		x[19].ElapsedMS = elapsed
		if _, err := summarize("sqlite", 100, "Q6GroupedCount", "test", sample{}, x); err == nil {
			t.Fatal("invalid elapsed value qualified")
		}
	}
	if _, err := summarize("sqlite", 100, "Q6GroupedCount", "test", sample{}, base[:19]); err == nil {
		t.Fatal("nineteen completed calls qualified")
	}
}

// This control uses main's public fallback composition. It proves snapshot
// pinning during a real concurrent publication, not W3's compiled final fence.
func TestPendingProducerKeepsHistoricalBodiesPinnedDuringPublication(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 33)
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			mutate, called := false, 0
			cfg := viewops.Config{
				OntologyRevision: model.Natural64(1),
				AuthorizeRead: func(ctx context.Context, p model.PrincipalRef, ref model.ResourceRef, _ string) error {
					_, err := f.app.Service.Get(ctx, p, service.GetRequest{ID: ref.Ref})
					return err
				},
				AuthorizeReadBatch: f.app.Service.AuthorizeReadBatch,
				ParentProjection: &viewops.ParentProjectionConfig{
					Project: func(ctx context.Context, p model.PrincipalRef, r driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
						a, err := f.app.Checker.AuthorizeParentProjections(ctx, p, r, refs)
						out := viewops.ParentProjectionAuthority{PolicyBinding: a.PolicyBinding, Parents: make(map[string]viewops.ParentDisclosure, len(a.Parents))}
						for id, value := range a.Parents {
							out.Parents[id] = viewops.ParentDisclosure{Exists: value.Exists, Ref: value.Ref}
						}
						return out, err
					},
					ValidateAuthority: f.app.Checker.ValidateReadAuthorityBinding,
				},
			}
			freshViews, err := viewops.New(f.store.Store, f.lease, cfg)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Readiness = qualificationReadiness(func(ctx context.Context, r driver.Reader, refs []model.Ref) (map[string]viewops.ReadinessState, error) {
				called++
				if r.Snapshot().At.String() != before.Head.String() || len(refs) != f.n {
					t.Fatal("pending producer lost complete pinned content")
				}
				if mutate {
					changed := document(1)
					changed.Fields["title"] = "outside"
					encoded, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := conformance.Seed(context.Background(), f.store.Store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: beadRef(1), Kind: "bead", Op: model.OpPatch, Body: encoded, ChangedPaths: []string{"/fields/title"}}}); err != nil {
						t.Fatal(err)
					}
				}
				rows, err := r.Get(ctx, refs)
				if err != nil || len(rows) != f.n {
					t.Fatalf("pending pinned source: %v", err)
				}
				states := map[string]viewops.ReadinessState{"outside-page": {Pending: true}}
				for i, row := range rows {
					doc, err := pipeline.DecodeDocument(row.Body)
					if err != nil || doc.Fields["title"] != document(i + 1).Fields["title"] {
						t.Fatalf("pending mixed current source into pinned content: %+v %v", doc, err)
					}
					states[row.Ref.ID] = viewops.ReadinessState{Pending: true}
				}
				return states, nil
			})
			views, err := viewops.New(f.store.Store, f.lease, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, interference := range []bool{false, true} {
				mutate = interference
				var work counters
				got, err := views.Run(driver.WithReadWorkObserver(context.Background(), &work), f.user(1), groupedRequest())
				if err != nil || got.AsOf.Revision != before.Head || len(got.Groups) != 1 || got.Groups[0].Count != f.n || len(got.Groups[0].Items) != f.n || got.Count != nil || work.value().Queries == 0 {
					t.Fatalf("pinned publication result: %+v %v work=%+v", got, err, work.work())
				}
				for i, item := range got.Groups[0].Items {
					if !item.ReadinessPending || item.ID != beadRef(i+1).ID || item.Fields["title"] != document(i + 1).Fields["title"] {
						t.Fatal("pending content/order changed")
					}
				}
			}
			if called != 2 {
				t.Fatalf("pending called=%d", called)
			}
			fresh, err := freshViews.Run(context.Background(), f.user(1), groupedRequest())
			if err != nil || fresh.AsOf.Revision == before.Head || len(fresh.Groups) != 1 || fresh.Groups[0].Count != f.n-1 || len(fresh.Groups[0].Items) != f.n-1 || fresh.Groups[0].Items[0].ID != beadRef(2).ID {
				t.Fatalf("fresh publication membership: %+v %v", fresh, err)
			}
			t.Logf("CP7_PENDING_SNAPSHOT engine=%s control_and_commit_interference=true pinned_content_and_membership=true fresh_membership=true compiled_fence=UNMEASURED", engine)
		})
	}
}
