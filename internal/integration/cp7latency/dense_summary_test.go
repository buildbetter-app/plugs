package cp7latency_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
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

func checkCompiledSummaryPlan(t *testing.T, f *fixture, plans []driver.SelectionPlan) {
	t.Helper()
	if len(plans) != 1 {
		t.Fatalf("summary did not execute exactly one storage selection: %d", len(plans))
	}
	plan := plans[0]
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	if plan.Mode != driver.SelectionTypeSummary || plan.Cap != viewops.TypeSummaryCandidateCap || plan.GroupBy != driver.SelectionGroupType || plan.Page != nil || !plan.ContentCommitted {
		t.Fatalf("summary did not use its complete bounded storage plan: %+v", plan)
	}
	description, err := plan.Boundary.Description()
	if err != nil || description.Chain != driver.SelectionChainCompiled || description.Host != driver.SelectionHostAbsent || description.InlineViewBinding == "" {
		t.Fatalf("summary lacks its actual compiled inline authority: %+v %v", description, err)
	}
	rows, err := authzfixed.SelectionRows(f.base.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &rows.Source})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := authzfixed.FactsSourceBytes(f.base)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range description.Sources {
		observed, err := driver.EncodeLockKey(source.Key)
		if err != nil {
			t.Fatal(err)
		}
		if observed == key && source.Present && source.Digest == driver.SelectionDigest(canonical) {
			return
		}
	}
	t.Fatal("storage plan lacks the published canonical facts record")
}

func TestCP7CompiledSummaryUsesPublishedAuthorityAfterRecompose(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, seedBatch)
			ctx := context.Background()
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var plans []driver.SelectionPlan
			f.viewPlan = func(plan driver.SelectionPlan) { plans = append(plans, plan) }
			f.compose(t)
			var counts counters
			got, err := f.views.Run(driver.WithReadWorkObserver(ctx, &counts), f.user(1), viewops.RunRequest{Project: projectName, Definition: &viewops.Definition{Filter: &viewops.Filter{Query: viewQuery}, Fields: []string{}, GroupBy: []string{"type"}}})
			if err != nil {
				t.Fatal(err)
			}
			checkCompiledSummaryPlan(t, f, plans)
			if len(got.Items) != 0 || got.NextCursor != nil || got.Count != nil || len(got.Groups) != 1 || got.Groups[0].Key != "issue" || got.Groups[0].Count != seedBatch || len(got.Groups[0].Items) != 0 || got.Groups[0].Elided != nil {
				t.Fatalf("compiled summary differs from independently seeded population: %+v", got)
			}
			if counts.value().Queries != 0 || counts.value().Gets != 0 || counts.work().AggregateInputs != 0 || counts.work().Projections != 0 || counts.work().AggregateGroups != 1 {
				t.Fatalf("compiled summary used ordinary payload/aggregation work: api=%+v work=%+v", counts.value(), counts.work())
			}
			after, err := f.store.Meta(ctx)
			if err != nil || after.Head.String() != before.Head.String() {
				t.Fatalf("recomposition or compiled read republished authority: %v", err)
			}
		})
	}
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
			var plans []driver.SelectionPlan
			f.viewPlan = func(plan driver.SelectionPlan) { plans = append(plans, plan) }
			f.compose(t)
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
				plans = nil
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
				checkCompiledSummaryPlan(t, f, plans)
				if len(got.Items) != 0 || got.NextCursor != nil || got.Count != nil || len(got.Groups) != 1 || got.Groups[0].Key != "issue" || got.Groups[0].Count != n || len(got.Groups[0].Items) != 0 || got.Groups[0].Elided != nil {
					t.Fatalf("dense summary differs from independent corpus population: %+v", got)
				}
				// SQL input visits are not observer events. The plan and independent
				// population prove semantics; zero core visits forbids scan fallback.
				if counts.value().Queries != 0 || counts.value().Gets != 0 || counts.work().AggregateInputs != 0 || counts.work().Projections != 0 || counts.work().AggregateGroups != 1 {
					t.Fatalf("dense summary used ordinary payload/aggregation work: api=%+v work=%+v", counts.value(), counts.work())
				}
				t.Logf("CP7_DENSE_TYPE_SUMMARY fields_present=true fields_empty=true group_by=type finite_cp7=true authorized_inputs=%d groups=1 corpus_sha256=%s executed_work=%+v sql_executor_input_visits=UNMEASURED performance=UNMEASURED", n, f.corpusHash, counts.work())
			})
			after, err := f.store.Meta(ctx)
			if err != nil || before.Head.String() != after.Head.String() {
				t.Fatalf("dense read changed head: %v", err)
			}
			// The full-size indexed claim control remains in each explicit
			// 100k manual target. The ordinary cap+1 control cannot qualify
			// that full-size claim or the unchanged cold/3/20 latency matrix.
			if n == corpusSize {
				t.Run("CertifiedIndexedGuardClaimWork", func(t *testing.T) { checkCertifiedIndexedClaimWork(t, f) })
			}
		})
	}
}

func checkCertifiedIndexedClaimWork(t *testing.T, f *fixture) {
	t.Helper()
	if f.n != corpusSize {
		t.Fatal("indexed claim qualification requires the original full 100k corpus")
	}
	f.verifySourceCoverage(t, "w8_before_claim")
	req := service.ClaimNextRequest{Project: projectName}
	identity, err := service.ClaimNextRequestIdentity(req)
	if err != nil {
		t.Fatal(err)
	}
	var key string
	winner := 0
	// Preserve the raw seeded rotation. Choose a distinguishing root with
	// a real child, so validation must return/materialize source membership
	// and freshly Observe it, rather than pass with an empty index result.
	for i := 0; i < 128; i++ {
		key = requestKey("w8-indexed-claim", 1, "qualification", i)
		seed := sha256.Sum256([]byte(identity + "\x00" + key))
		group := sha256.Sum256([]byte(fmt.Sprintf("%x", seed) + "\x000"))
		candidate := int(binary.BigEndian.Uint64(group[:8])%uint64(f.n)) + 1
		if (candidate-1)%11 == 0 && candidate < f.n {
			winner = candidate
			break
		}
	}
	if winner == 0 {
		t.Fatal("no raw pivot selecting a root with a child")
	}
	f.store.counts.reset()
	deadline, cancel := context.WithTimeout(context.Background(), requestDeadline)
	defer cancel()
	ctx := driver.WithReadWorkObserver(deadline, &f.store.counts)
	p := f.agent(1, false)
	meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: key}
	out, err := f.app.Service.ClaimNextWithOutcome(ctx, p, req, meta)
	if err != nil || out == nil || out.Replayed || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != beadRef(winner).ID {
		t.Fatalf("full corpus supported raw-pivot claim: expected=%s outcome=%+v err=%v", beadRef(winner).ID, out, err)
	}
	globalAPI, globalWork := f.store.counts.value(), f.store.counts.work()
	validationAPI, validationWork := f.store.counts.validation.value(), f.store.counts.validation.work()
	if validationAPI.Queries != 0 || validationAPI.Returned != 0 || validationWork.SourceQueries == 0 || validationWork.SourceHeaders == 0 || validationWork.SourceImages == 0 {
		t.Fatalf("full corpus claim did not execute certified indexed-only validation: api=%+v work=%+v", validationAPI, validationWork)
	}
	// Stored source Scan + returned body clone + fresh source Observe/Get
	// are repeated physical materializations of the same membership image.
	// This lower bound is deliberately independent of unique graph IDs.
	if validationWork.Payloads < 3*validationWork.SourceImages || validationWork.PayloadBytes < 3*int64(len(body(t, winner+1))) {
		t.Fatalf("indexed membership copies/fresh observations escaped accounting: %+v", validationWork)
	}
	// A queue projection can select without raw Query. Its complete live
	// census must still expose at least one key and decoded header state per
	// corpus bead, plus actual bytes, outside ontology validation. These are
	// lower bounds on counted O(N) thin work, not bounded-work/SLO proof.
	if globalAPI.Queries == 0 {
		selectionKeys := globalWork.SelectorKeys - validationWork.SelectorKeys
		selectionStates := globalWork.PrefixStates - validationWork.PrefixStates
		selectionBytes := globalWork.PrefixBytes - validationWork.PrefixBytes
		if selectionKeys < int64(f.n) || selectionStates < int64(f.n) || selectionBytes <= 0 {
			t.Fatalf("claim selection did not expose raw Query or a counted complete census: global=%+v/%+v validation=%+v/%+v", globalAPI, globalWork, validationAPI, validationWork)
		}
	}
	if globalAPI.Queries < validationAPI.Queries || globalWork.Payloads < validationWork.Payloads || globalWork.SourceImages < validationWork.SourceImages {
		t.Fatalf("raw claim selection/global work was hidden: global=%+v/%+v validation=%+v/%+v", globalAPI, globalWork, validationAPI, validationWork)
	}
	if globalAPI.Observed != nil || validationAPI.Observed != nil {
		t.Fatal("unmeasured observed-key count was invented")
	}
	f.checkClaim(t, p, out, meta, 1)
	t.Logf("CP7_W8_INDEXED_CLAIM beads=%d expected_raw_winner=%s global_api=%+v global_work=%+v validation_api=%+v validation_work=%+v payload_basis=instrumented_driver_sourceindex_materializations source_images_basis=reconstructed_membership_images core_snapshot_output_clones=UNMEASURED unhooked_metadata_coverage_lock_commit_work=UNMEASURED sql_executor_work=UNMEASURED performance=UNMEASURED", f.n, beadRef(winner).ID, globalAPI, globalWork, validationAPI, validationWork)
	f.resetClaim(t, out.Commit.Results[0].Ref)
	f.verifySourceCoverage(t, "w8_after_claim_reset")
	f.verifyCorpus(t)
}
