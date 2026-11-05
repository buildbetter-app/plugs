package cp7latency_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const ordinaryDenseCorpusSize = viewops.OrdinaryCandidateCap + 1

func denseCorpusSize(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("CP7_DENSE_CORPUS_SIZE")
	if raw == "" {
		return ordinaryDenseCorpusSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("invalid CP7_DENSE_CORPUS_SIZE %q: %v", raw, err)
	}
	if n != ordinaryDenseCorpusSize && n != corpusSize {
		t.Fatalf("unsupported CP7_DENSE_CORPUS_SIZE %d", n)
	}
	return n
}

func denseEngines(t *testing.T) []string {
	t.Helper()
	if engine := os.Getenv("CP7_DENSE_ENGINE"); engine != "" {
		if engine != "sqlite" && engine != "postgres" {
			t.Fatalf("unsupported CP7_DENSE_ENGINE %q", engine)
		}
		return []string{engine}
	}
	return []string{"sqlite", "postgres"}
}

// These are semantic completions, like the original correctness checks using
// Background. They are not timed benchmark samples. The original Q6 request,
// 30s measurement deadline, fractions and cold/3/20 matrix remain unchanged.
func TestDenseOrdinaryQ6RefusalAndSeparateDecision20TypeSummary(t *testing.T) {
	checkDeclaration(t)
	if viewops.OrdinaryCandidateCap != 20000 || viewops.TypeSummaryCandidateCap != 100000 || viewops.MaxGroups != 100 {
		t.Fatal("decision #20 bounds changed")
	}
	n := denseCorpusSize(t)
	if n != ordinaryDenseCorpusSize && n != corpusSize {
		t.Fatalf("dense semantic corpus must be cap control or full corpus, got %d", n)
	}
	engines := denseEngines(t)
	t.Logf("CP7_DENSE_SELECTION corpus=%d engines=%v ordinary_control_corpus=%d full_manual_corpus=%d", n, engines, ordinaryDenseCorpusSize, corpusSize)
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, n)
			ctx := context.Background()
			p := f.user(1)
			// Prove finite compiler eligibility with the actual installed CP7
			// chain and host caller. No no-plugin or synthetic authorizer shortcut.
			caller, err := authzrpc.DefaultCallerSource(model.Natural64(1))(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			template, err := f.app.Chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.get"})
			if err != nil || template.NoPlugins {
				t.Fatalf("finite CP7 compiler eligibility: %v", err)
			}
			if err := template.Validate(); err != nil {
				t.Fatal(err)
			}
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Run("OriginalDenseQ6StillRefuses20000Cap", func(t *testing.T) {
				request := groupedRequest()
				if len(request.Definition.Fields) == 0 {
					t.Fatal("original dense Q6 relabeled as summary")
				}
				got, err := f.views.Run(ctx, p, request)
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "scope_too_large" || fmt.Sprint(typed.Data.Details["cap"]) != "20000" {
					t.Fatalf("dense ordinary request must refuse at 20k: %v", err)
				}
				if len(got.Items) != 0 || len(got.Groups) != 0 || got.NextCursor != nil || got.Count != nil {
					t.Fatal("dense ordinary refusal emitted partial output")
				}
				t.Logf("CP7_DENSE_ORDINARY authorized_inputs=%d cap=20000 outcome=scope_too_large performance=RED", n)
			})
			t.Run("Decision20ExplicitEmptyFieldsDenseTypeSummary", func(t *testing.T) {
				// Decode an actually present fields=[] member, distinct from
				// omitted fields and from the original nonempty Q6 definition.
				var definition viewops.Definition
				if err := json.Unmarshal([]byte(`{"filter":{"query":"type=issue AND title=CP7"},"fields":[],"groupBy":["type"]}`), &definition); err != nil {
					t.Fatal(err)
				}
				if definition.Fields == nil || len(definition.Fields) != 0 || len(definition.GroupBy) != 1 || definition.GroupBy[0] != "type" || len(definition.Sort) != 0 {
					t.Fatal("summary definition lost explicit-empty eligibility")
				}
				var counts counters
				observed := driver.WithReadWorkObserver(ctx, &counts)
				got, err := f.views.Run(observed, p, viewops.RunRequest{Project: projectName, Definition: &definition})
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Items) != 0 || got.NextCursor != nil || got.Count != nil || len(got.Groups) != 1 || got.Groups[0].Key != "issue" || got.Groups[0].Count != n || len(got.Groups[0].Items) != 0 || got.Groups[0].Elided != nil {
					t.Fatalf("dense summary differs from independent corpus population: %+v", got)
				}
				if counts.work().AggregateInputs != int64(n) || counts.work().AggregateGroups != 1 {
					t.Fatalf("dense summary aggregate work hidden: %+v", counts.work())
				}
				t.Logf("CP7_DENSE_TYPE_SUMMARY fields_present=true fields_empty=true group_by=type finite_cp7=true authorized_inputs=%d groups=1 corpus_sha256=%s executed_work=%+v performance=UNMEASURED", n, f.corpusHash, counts.work())
			})
			after, err := f.store.Meta(ctx)
			if err != nil || before.Head.String() != after.Head.String() {
				t.Fatalf("dense read changed head: %v", err)
			}
		})
	}
}
