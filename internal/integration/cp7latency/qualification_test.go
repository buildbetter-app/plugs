package cp7latency_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

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

func TestCompiledGroupedEveryPageMatchesIndependentCorpus(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 1000)
			views := qualificationViews(t, f, nil)
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
						if work.work().Selections != 1 || work.value().Queries != 0 {
							t.Fatalf("compiled page escaped Select: %+v %+v", work.work(), work.value())
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

func TestCompiledNoPayloadResultsFenceRevocationAfterSelection(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"summary", "count", "empty_count"} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				f := newFixture(t, engine, 33)
				req := summaryRequest()
				if mode != "summary" {
					req.CountOnly, req.Definition.GroupBy = true, nil
					if mode == "empty_count" {
						req.Definition.Filter.Query = "type=issue AND title=absent"
					}
				}
				var once sync.Once
				fenced := false
				views := qualificationViews(t, f, func() {
					once.Do(func() {
						fenced = true
						next := f.base
						next.Grants = nil
						if err := f.facts.Replace(next); err != nil {
							t.Fatal(err)
						}
					})
				})
				var work counters
				got, err := views.Run(driver.WithReadWorkObserver(context.Background(), &work), f.user(1), req)
				if !fenced || err == nil || !reflect.DeepEqual(got, viewops.Result{}) || work.work().Selections != 1 {
					t.Fatalf("revoked final authority published a partial result: %+v %v work=%+v", got, err, work.work())
				}
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "authz_unavailable" {
					t.Fatalf("wrong race refusal: %v", err)
				}
				fresh := qualificationViews(t, f, nil)
				got, err = fresh.Run(context.Background(), f.user(1), req)
				want := f.n
				if mode == "empty_count" {
					want = 0
				}
				if err != nil || len(got.Items) != 0 || got.NextCursor != nil || mode == "summary" && (got.Count != nil || len(got.Groups) != 1 || got.Groups[0].Count != want) || mode != "summary" && (len(got.Groups) != 0 || got.Count == nil || *got.Count != want) {
					t.Fatalf("fresh revoked authority: %+v %v", got, err)
				}
				t.Logf("CP7_FINAL_FENCE engine=%s mode=%s selected_before_revoke=true no_partial_result=true fresh_authority=true", engine, mode)
			})
		}
	}
}

func TestSummaryP95RequiresTwentyCompleteFiniteObservations(t *testing.T) {
	base := make([]sample, measuredSamples)
	for i := range base {
		base[i] = sample{Engine: "sqlite", FractionDivisor: 100, Corpus: corpusSize, ExpectedAuthorized: corpusSize / 100, Operation: "Q6TypeSummary", Phase: "measured", Index: i, Status: "success", ElapsedMS: 249}
	}
	good, err := summarize("sqlite", 100, "Q6TypeSummary", "test", sample{}, base)
	if err != nil || !good.TargetPass || good.TargetMS != 250 || good.SuccessfulP95MS == nil {
		t.Fatalf("complete summary control: %+v %v", good, err)
	}
	for _, status := range []string{"cap_refusal", "request_deadline", "write_pre_deadline"} {
		x := append([]sample(nil), base...)
		x[19].Status = status
		got, err := summarize("sqlite", 100, "Q6TypeSummary", "test", sample{}, x)
		if err != nil || got.TargetPass || got.SuccessfulP95MS != nil || got.Status != "RED" {
			t.Fatalf("incomplete successful summary qualified: %+v %v", got, err)
		}
	}
	for _, elapsed := range []float64{-1, math.NaN(), math.Inf(1)} {
		x := append([]sample(nil), base...)
		x[19].ElapsedMS = elapsed
		if _, err := summarize("sqlite", 100, "Q6TypeSummary", "test", sample{}, x); err == nil {
			t.Fatal("invalid elapsed value qualified")
		}
	}
	if _, err := summarize("sqlite", 100, "Q6TypeSummary", "test", sample{}, base[:19]); err == nil {
		t.Fatal("nineteen completed calls qualified")
	}
}
