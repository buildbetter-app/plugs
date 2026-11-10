package cp7latency_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Deliberately omit the optional hierarchy capability only in this fallback
// control. Product composition still receives the concrete store/readers.
type accountingScanSnapshot struct{ driver.Reader }

// This models the sealed observer interface, without accidentally promoting
// ScopedReadWorkObserver from counters through embedding.
type accountingLegacyObserver struct{ counts counters }

func (o *accountingLegacyObserver) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	o.counts.ObserveReadWork(kind, amount)
}

func TestIndexedSourceWorkCountsRepeatedRelationsAndObservations(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			linked := document(3)
			linked.Links = []pipeline.Link{{Kind: "parent-child", To: beadRef(2)}}
			encoded, err := json.Marshal(linked)
			if err != nil {
				t.Fatal(err)
			}
			f.patchSource(t, "accounting_relations", beadRef(3), encoded, []string{"/links"})
			func() {
				r := f.reader(t, nil)
				defer r.Close()
				children, ok := r.(driver.HierarchyChildrenExcludingReader)
				if !ok {
					t.Fatal("canonical source capability hidden")
				}
				inverse, ok := r.(driver.InverseSourceLinksReader)
				if !ok {
					t.Fatal("inverse source capability hidden")
				}
				var counts counters
				ctx := driver.WithReadWorkObserver(context.Background(), &counts)
				checkCopies := func(phase string, payloads, images int64) {
					t.Helper()
					work := counts.work()
					if work.Payloads != payloads || work.PayloadBytes != payloads*int64(len(encoded)) || work.SourceImages != images || counts.value().Queries != 0 {
						t.Fatalf("%s repeated source materializations: api=%+v work=%+v want_payloads=%d want_images=%d", phase, counts.value(), work, payloads, images)
					}
				}
				rows, more, err := children.HierarchyChildren(ctx, []model.Ref{beadRef(2)}, 1)
				if err != nil || more || len(rows) != 1 || rows[0].Ref.ID != beadRef(3).ID || !bytes.Equal(rows[0].Body, encoded) {
					t.Fatalf("canonical source image: rows=%+v more=%v err=%v", rows, more, err)
				}
				checkCopies("canonical", 2, 1)
				rows, more, err = inverse.InverseSourceLinks(ctx, []model.Ref{beadRef(2)}, []driver.IncomingLinkKind{driver.IncomingLinkParentChild}, nil, 1)
				if err != nil || more || len(rows) != 1 || rows[0].Ref.ID != beadRef(3).ID || !bytes.Equal(rows[0].Body, encoded) {
					t.Fatalf("same inverse source image: rows=%+v more=%v err=%v", rows, more, err)
				}
				checkCopies("inverse", 4, 2)
				rows, more, err = children.HierarchyChildren(ctx, []model.Ref{beadRef(2)}, 1)
				if err != nil || more || len(rows) != 1 || rows[0].Ref.ID != beadRef(3).ID || !bytes.Equal(rows[0].Body, encoded) {
					t.Fatalf("repeated canonical source image: rows=%+v more=%v err=%v", rows, more, err)
				}
				checkCopies("canonical_repeat", 6, 3)
				ref := beadRef(3)
				states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}})
				if err != nil || len(states) != 1 || !states[0].Present || states[0].Record == nil || !bytes.Equal(states[0].Record.Body, encoded) {
					t.Fatalf("fresh source observation: states=%+v err=%v", states, err)
				}
				checkCopies("observe", 7, 3)
				getCopies, duplicateCopies := int64(1), int64(2)
				if engine == "postgres" {
					// PostgreSQL reconstructs each stored key once, then clones
					// its body for each requested position, including duplicates.
					getCopies, duplicateCopies = 2, 3
				}
				rows, err = r.Get(ctx, []model.Ref{ref})
				if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, encoded) {
					t.Fatalf("fresh source Get: rows=%+v err=%v", rows, err)
				}
				checkCopies("get", 7+getCopies, 3)
				rows, err = r.Get(ctx, []model.Ref{ref, ref})
				if err != nil || len(rows) != 2 || !bytes.Equal(rows[0].Body, encoded) || !bytes.Equal(rows[1].Body, encoded) {
					t.Fatalf("duplicate source Get: rows=%+v err=%v", rows, err)
				}
				checkCopies("duplicate_get", 7+getCopies+duplicateCopies, 3)
				missing := beadRef(12)
				states, err = r.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &missing}})
				if err != nil || len(states) != 1 || states[0].Present || states[0].Record != nil || !states[0].Revision.IsZero() {
					t.Fatalf("missing source observation: states=%+v err=%v", states, err)
				}
				checkCopies("missing", 7+getCopies+duplicateCopies, 3)
				states, err = r.Observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock})
				if err != nil || len(states) != 1 || states[0].Record != nil {
					t.Fatalf("metadata observation: states=%+v err=%v", states, err)
				}
				checkCopies("metadata", 7+getCopies+duplicateCopies, 3)
				if counts.work().SourceHeaders < 3 || counts.work().SourceQueries < 6 || counts.value().Gets != 2 || counts.value().Observed != nil || counts.validation.work() != (executedReadWork{}) {
					t.Fatalf("repeated relation work/scopes: api=%+v work=%+v validation=%+v", counts.value(), counts.work(), counts.validation.work())
				}
				t.Logf("CP7_REPEATED_SOURCE_WORK distinct_source_ids=1 source_images=3 payload_materializations=%d payload_body_bytes=%d duplicate_get=true missing_payloads=0 metadata_payloads=0 payload_basis=instrumented_driver_sourceindex_materializations core_snapshot_output_clones=UNMEASURED observed_keys=UNMEASURED sql_executor_work=UNMEASURED", counts.work().Payloads, counts.work().PayloadBytes)
			}()
			f.patchSource(t, "accounting_relations_reset", beadRef(3), body(t, 3), []string{"/links"})
			f.verifyCorpus(t)
		})
	}
}

func TestOntologyValidationScopeReachesSQLFallbackAndDoesNotLeak(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			r := f.reader(t, nil)
			defer r.Close()
			rows, err := r.Get(context.Background(), []model.Ref{beadRef(1)})
			if err != nil || len(rows) != 1 {
				t.Fatalf("scope source: %v", err)
			}
			doc, err := pipeline.DecodeDocument(rows[0].Body)
			if err != nil {
				t.Fatal(err)
			}
			doc.Fields["status"] = "in_progress"
			after, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			p := port.ProposedChange{Principal: f.agent(1, false), BaseRevision: *r.Snapshot().At,
				Meta: model.WriteMeta{Project: projectName, Method: "bead.claim"},
				Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead", Ref: beadRef(1)},
					Op: model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: beadRef(1), Fields: map[string]any{}}}, Before: rows[0].Body, After: after}}}
			o := service.NewOntology(nil)
			snapshot := accountingScanSnapshot{Reader: r}
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			// Identical real validation/SQL inputs, with no observer, nil
			// observer, old observer, and the new optional scoped observer.
			if err := o.Validate(context.Background(), p, snapshot); err != nil {
				t.Fatal(err)
			}
			if err := o.Validate(driver.WithReadWorkObserver(context.Background(), nil), p, snapshot); err != nil {
				t.Fatal(err)
			}
			var legacy accountingLegacyObserver
			if err := o.Validate(driver.WithReadWorkObserver(context.Background(), &legacy), p, snapshot); err != nil {
				t.Fatal(err)
			}
			var counts counters
			ctx := driver.WithReadWorkObserver(context.Background(), &counts)
			if err := o.Validate(ctx, p, snapshot); err != nil {
				t.Fatal(err)
			}
			if legacy.counts.value() != counts.value() || legacy.counts.work() != counts.work() {
				t.Fatalf("optional scope changed or doubled global work: old=%+v/%+v new=%+v/%+v", legacy.counts.value(), legacy.counts.work(), counts.value(), counts.work())
			}
			if counts.validation.value().Queries == 0 || counts.validation.value().Returned != 11 || counts.validation.work().Payloads < 11 || counts.validation.work() != counts.work() || counts.validation.value() != counts.value() {
				t.Fatalf("real fallback SQL work did not inherit Validate scope: global=%+v/%+v scoped=%+v/%+v", counts.value(), counts.work(), counts.validation.value(), counts.validation.work())
			}
			api, work := counts.validation.value(), counts.validation.work()
			if _, err := r.Get(ctx, []model.Ref{beadRef(1)}); err != nil {
				t.Fatal(err)
			}
			if counts.validation.value() != api || counts.validation.work() != work || counts.value().Gets != api.Gets+1 || counts.work().Payloads <= work.Payloads {
				t.Fatal("validation scope leaked into the caller's subsequent raw Get")
			}
			current, err := f.store.Meta(context.Background())
			if err != nil || current.Head.String() != before.Head.String() {
				t.Fatal("observational validation control published state")
			}
			t.Logf("CP7_VALIDATION_SCOPE fallback_query_calls=%d old_global_equals_scoped_global=true scope_leak=false sql_executor_work=UNMEASURED", api.Queries)
		})
	}
}

func TestExecutedReadWorkOnRealSQLReadersAndTransactions(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			r := f.reader(t, nil)
			defer r.Close()
			q := driver.Query{Filter: map[string]any{"kind": "bead", "id": beadRef(11).ID}, Limit: 1}
			plain, err := r.Query(context.Background(), q)
			if err != nil || len(plain.Items) != 1 || plain.Items[0].Ref.ID != beadRef(11).ID {
				t.Fatalf("unobserved control: %v", err)
			}
			nilObserved, err := r.Query(driver.WithReadWorkObserver(context.Background(), nil), q)
			if err != nil || !reflect.DeepEqual(plain, nilObserved) {
				t.Fatalf("nil observer changed query: %v", err)
			}
			var counts counters
			ctx := driver.WithReadWorkObserver(context.Background(), &counts)
			observed, err := r.Query(ctx, q)
			if err != nil || !reflect.DeepEqual(plain, observed) {
				t.Fatalf("observer changed query: %v", err)
			}
			invalid := driver.Query{Filter: map[string]any{"unsupported": "value"}}
			_, plainErr := r.Query(context.Background(), invalid)
			_, observedErr := r.Query(ctx, invalid)
			if plainErr == nil || observedErr == nil || plainErr.Error() != observedErr.Error() {
				t.Fatal("observer changed query refusal")
			}
			work, api := counts.work(), counts.value()
			var corpusBytes int64
			for i := 1; i <= f.n; i++ {
				corpusBytes += int64(len(body(t, i)))
			}
			if api.Queries != 2 || api.Returned != 1 || work.SelectorKeys < int64(f.n) || work.Payloads < int64(f.n) || work.PayloadBytes < corpusBytes || work.Payloads <= api.Returned {
				t.Fatalf("discarded candidates' executed work missing: api=%+v work=%+v", api, work)
			}
			// No query/driver wrapper is introduced by accounting. In particular,
			// the SQL reader's prefix capability is unchanged by observer use.
			if engine == "sqlite" {
				if _, ok := r.(driver.DerivedPrefixReader); !ok {
					t.Fatal("SQLite prefix capability hidden")
				}
			}
			counts.reset()
			rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
			if err != nil || len(rows) != 66 || counts.work().PrefixStates != 66 || counts.work().PrefixBytes == 0 {
				t.Fatalf("real prefix decode work: rows=%d work=%+v err=%v", len(rows), counts.work(), err)
			}
			// A real WriteTx.Reader sees the same observer. Old Read-only wrappers
			// could not count this work and hid optional transaction capabilities.
			tx, err := f.store.Store.Write(ctx, f.lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, ok := tx.(driver.AllocatorContinuation); !ok {
				t.Fatal("allocator transaction capability hidden")
			}
			if _, ok := tx.(driver.HashAllocatorContinuation); !ok {
				t.Fatal("hash allocator transaction capability hidden")
			}
			if _, ok := tx.(driver.CheckpointGuardTx); !ok {
				t.Fatal("checkpoint transaction capability hidden")
			}
			counts.reset()
			page, err := tx.Query(ctx, q)
			if err != nil || len(page.Items) != 1 || counts.value().Queries != 1 || counts.work().Payloads < int64(f.n) {
				t.Fatalf("transaction query work escaped accounting: %+v %v", counts.work(), err)
			}
			counts.reset()
			values, err := tx.Get(ctx, []driver.Ref{beadRef(11)})
			getCopies := int64(1)
			if engine == "postgres" {
				getCopies = 2
			}
			if err != nil || len(values) != 1 || counts.value().Gets != 1 || counts.work().Payloads != getCopies || counts.work().PayloadBytes != getCopies*int64(len(values[0].Body)) {
				t.Fatalf("transaction Get work escaped accounting: %+v %v", counts.work(), err)
			}
			// A staged record is an alias until the driver constructs each
			// caller-owned output. Two duplicate refs must charge two copies.
			ref := beadRef(11)
			bead, err := driver.NewBeadSentinel(ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: bead}, driver.GraphBlockingSentinelLock}
			sort.Slice(keys, func(i, j int) bool {
				a, _ := driver.EncodeLockKey(keys[i])
				b, _ := driver.EncodeLockKey(keys[j])
				return a < b
			})
			locks := make([]driver.LockRequest, len(keys))
			for i, key := range keys {
				locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
			}
			if _, err := tx.Lock(ctx, locks); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if err := pipeline.PutSourceRecord(ctx, tx, values[0]); err != nil {
				t.Fatal(err)
			}
			counts.reset()
			values, err = tx.Get(ctx, []driver.Ref{ref, ref})
			if err != nil || len(values) != 2 || !bytes.Equal(values[0].Body, body(t, 11)) || !bytes.Equal(values[1].Body, body(t, 11)) || counts.value().Gets != 1 || counts.work().Payloads != 2 || counts.work().PayloadBytes != 2*int64(len(body(t, 11))) {
				t.Fatalf("staged duplicate Get copies escaped accounting: %+v %v", counts.work(), err)
			}
			t.Logf("CP7_EXECUTED_WORK query_output=1 materialized_at_least=%d transaction_query_and_get=true staged_duplicate_get_payloads=2 prefix_states=66 sql_executor_work=UNMEASURED", f.n)
		})
	}
}

func TestReadWorkConcurrentRequestsStayIsolated(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			var observed, untouched counters
			ctx := driver.WithReadWorkObserver(context.Background(), &observed)
			other := driver.WithReadWorkObserver(context.Background(), &untouched)
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				r := f.reader(t, nil)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer r.Close()
					_, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1})
					errs <- err
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			if observed.value().Queries != 4 || observed.value().Returned != 4 || observed.work().Payloads < 44 || untouched.value().Queries != 0 || untouched.work() != (executedReadWork{}) {
				t.Fatal("concurrent observer lost work or leaked across requests")
			}
			r := f.reader(t, nil)
			defer r.Close()
			if _, err := r.Query(other, driver.Query{Limit: 1}); err != nil {
				t.Fatal(err)
			}
			if observed.value().Queries != 4 || untouched.value().Queries != 1 {
				t.Fatal("request-local observer changed another request")
			}
		})
	}
}

func TestAggregateWorkIncludesNonEmittedSummaryProjections(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			var plans []driver.SelectionPlan
			f.viewPlan = func(plan driver.SelectionPlan) { plans = append(plans, plan) }
			f.compose(t)
			var counts counters
			ctx := driver.WithReadWorkObserver(context.Background(), &counts)
			compiled, err := f.views.Run(ctx, f.user(1), groupedRequest())
			if err != nil {
				t.Fatal(err)
			}
			f.checkGrouped(t, compiled, 1)
			if len(plans) != 1 || plans[0].Validate() != nil || plans[0].Mode != driver.SelectionPageMode || plans[0].Cap != viewops.OrdinaryCandidateCap || plans[0].GroupBy != driver.SelectionGroupType {
				t.Fatalf("ordinary grouping did not use its real SQL plan: %+v", plans)
			}
			if work := counts.work(); work.AggregateInputs != 0 || work.AggregateGroups != 1 || work.Projections != 11 || counts.value().Queries != 0 {
				t.Fatalf("compiled grouping hid core work or scanned: %+v %+v", counts.value(), work)
			}
			counts.reset()
			plans = nil
			summaryRequest := groupedRequest()
			summaryRequest.Definition.Fields = []string{}
			summaryRequest.Definition.Sort = nil
			compiledSummary, err := f.views.Run(ctx, f.user(1), summaryRequest)
			if err != nil || len(plans) != 1 || plans[0].Validate() != nil || plans[0].Mode != driver.SelectionTypeSummary || plans[0].Cap != viewops.TypeSummaryCandidateCap {
				t.Fatalf("compiled summary plan: %+v %v", plans, err)
			}
			if work := counts.work(); work.AggregateInputs != 0 || work.AggregateGroups != 1 || work.Projections != 0 || counts.value().Queries != 0 || counts.value().Gets != 0 {
				t.Fatalf("compiled summary performed core payload work: %+v %+v", counts.value(), work)
			}
			// This control measures core aggregation, including discarded item
			// projections. Keep real current authorization and its final fence,
			// but explicitly omit SQL selection on this dedicated bound reader.
			// Compiled SQL aggregation is covered separately and never reported
			// as fabricated core input visits.
			selection := f.app.Service.ViewSelection()
			bind := selection.Bind
			bound := 0
			selection.Bind = func(ctx context.Context, by model.PrincipalRef, definition string) (*viewops.SelectionAuthority, error) {
				authority, err := bind(ctx, by, definition)
				if err == nil {
					bound++
					authority.Reader = accountingScanSnapshot{Reader: authority.Reader}
				}
				return authority, err
			}
			f.configureViews(t, selection)
			counts.reset()
			got, err := f.views.Run(ctx, f.user(1), groupedRequest())
			if err != nil {
				t.Fatal(err)
			}
			f.checkGrouped(t, got, 1)
			if !reflect.DeepEqual(got.Groups, compiled.Groups) || got.AsOf != compiled.AsOf {
				t.Fatal("ordinary fallback and SQL grouping changed output or snapshot")
			}
			ordinary := counts.work()
			if ordinary.AggregateInputs != 11 || ordinary.AggregateGroups != 1 || ordinary.Projections != 22 {
				t.Fatalf("ordinary executed projection/aggregate work=%+v", ordinary)
			}
			counts.reset()
			req := groupedRequest()
			req.Definition.Fields = []string{}
			req.Definition.Sort = nil
			got, err = f.views.Run(ctx, f.user(1), req)
			if err != nil || len(got.Groups) != 1 || got.Groups[0].Count != 11 || len(got.Groups[0].Items) != 0 {
				t.Fatalf("summary: %+v %v", got, err)
			}
			summary := counts.work()
			// The core summary now returns before the item loop. Requiring a
			// projection here would invent work that was removed by that path.
			if summary.AggregateInputs != 11 || summary.AggregateGroups != 1 || summary.Projections != 0 {
				t.Fatalf("core summary input/group work or projection elision=%+v", summary)
			}
			if !reflect.DeepEqual(got.Groups, compiledSummary.Groups) || got.AsOf != compiledSummary.AsOf {
				t.Fatal("summary fallback and SQL grouping changed output or snapshot")
			}
			if bound != 2 || counts.value().Queries == 0 {
				t.Fatalf("core accounting control did not bind and scan: bound=%d api=%+v", bound, counts.value())
			}
			t.Logf("CP7_AGGREGATE_WORK ordinary=%+v fields_empty_summary=%+v emitted_summary_payload_rows=0", ordinary, summary)
		})
	}
}

// Main batches historical misses without repeating the current-row query.
// Count actual materializations, including the discarded current body, on
// every real call; duplicate output rows do not create another Scan.
func TestPostgresHistoricalReadCountsDiscardedCurrentBodies(t *testing.T) {
	f := newFixture(t, "postgres", 2)
	originalOne, originalTwo := body(t, 1), body(t, 2)
	currentOne := append(append([]byte(nil), originalOne...), ' ')
	at := f.seededAt.Revision
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: beadRef(1), Kind: "bead", Op: model.OpPatch, Body: currentOne, ChangedPaths: []string{"/fields/title"}},
		{Ref: beadRef(2), Kind: "bead", Op: model.OpDelete},
	}); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"Get", "Query", "Observe"} {
		t.Run(method, func(t *testing.T) {
			r := f.reader(t, &at)
			defer r.Close()
			read := func(ctx context.Context) ([]driver.Record, error) {
				switch method {
				case "Get":
					return r.Get(ctx, []model.Ref{beadRef(1), beadRef(2), beadRef(99), beadRef(1)})
				case "Query":
					page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"id": beadRef(1).ID}, Limit: 1})
					if err == nil && (page.AsOf != at || page.NextCursor != nil) {
						t.Fatal("historical query changed snapshot or invented a tail")
					}
					return page.Items, err
				default:
					keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: refPointer(beadRef(1))}, {Kind: driver.LockRecord, Ref: refPointer(beadRef(2))}, {Kind: driver.LockRecord, Ref: refPointer(beadRef(99))}}
					states, err := r.Observe(ctx, keys)
					if err != nil {
						return nil, err
					}
					if len(states) != 3 || !states[0].Present || !states[1].Present || states[2].Present || states[2].Record != nil || !states[2].Revision.IsZero() {
						t.Fatalf("historical observations changed hit/deleted/absence semantics: %+v", states)
					}
					return []driver.Record{*states[0].Record, *states[1].Record}, nil
				}
			}
			plain, err := read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := [][]byte{originalOne, originalTwo}
			if method == "Get" {
				want = append(want, originalOne)
			}
			if method == "Query" {
				want = want[:1]
			}
			if len(plain) != len(want) {
				t.Fatalf("historical output rows=%d want=%d", len(plain), len(want))
			}
			for i, row := range plain {
				if !reflect.DeepEqual(row.Body, want[i]) || row.Revision != at {
					t.Fatalf("historical body/snapshot changed: %+v", row)
				}
			}
			nilObserved, err := read(driver.WithReadWorkObserver(context.Background(), nil))
			if err != nil || !reflect.DeepEqual(plain, nilObserved) {
				t.Fatalf("nil observer changed historical result: %v", err)
			}
			var counts counters
			for repeat := int64(1); repeat <= 2; repeat++ {
				got, err := read(driver.WithReadWorkObserver(context.Background(), &counts))
				if err != nil || !reflect.DeepEqual(plain, got) {
					t.Fatalf("observer changed historical result: %v", err)
				}
				work := counts.work()
				// Every method scans one current body and reconstructs two
				// historical bodies. Get clones each requested hit; Query clones
				// both candidates before filtering. Observe retains those decoded
				// records directly and performs no additional body clone.
				copies, copyBytes := int64(0), int64(0)
				switch method {
				case "Get":
					copies, copyBytes = 3, int64(2*len(originalOne)+len(originalTwo))
				case "Query":
					copies, copyBytes = 2, int64(len(originalOne)+len(originalTwo))
				}
				wantPayloads := repeat * (3 + copies)
				wantBytes := repeat * (int64(len(currentOne)+len(originalOne)+len(originalTwo)) + copyBytes)
				if work.Payloads != wantPayloads || work.PayloadBytes != wantBytes {
					t.Fatalf("discarded/repeated historical work=%+v want_payloads=%d want_bytes=%d", work, wantPayloads, wantBytes)
				}
				t.Logf("CP7_HISTORICAL_BODY_WORK method=%s repeat=%d output_rows=%d current_body_scans_per_call=1 historical_bodies_per_call=2 absent=1 deleted_current_body=0 payload_records=%d payload_bytes=%d", method, repeat, len(got), work.Payloads, work.PayloadBytes)
			}
		})
	}
}

func refPointer(ref model.Ref) *model.Ref { return &ref }

func TestPostgresReadWorkPreservesStagedAndErrorPrecedence(t *testing.T) {
	f := newFixture(t, "postgres", 2)
	tx, err := f.store.Write(context.Background(), f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ref := beadRef(1)
	bead, err := driver.NewBeadSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: bead}, driver.GraphBlockingSentinelLock}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err := tx.Lock(context.Background(), locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	staged := append(append([]byte(nil), body(t, 1)...), ' ')
	if err := pipeline.PutSourceRecord(context.Background(), tx, driver.Record{Ref: ref, Kind: "bead", Body: staged}); err != nil {
		t.Fatal(err)
	}
	refs := []model.Ref{ref, beadRef(99), ref}
	plain, err := tx.Get(context.Background(), refs)
	if err != nil || len(plain) != 2 || !reflect.DeepEqual(plain[0].Body, staged) {
		t.Fatalf("staged control: %+v %v", plain, err)
	}
	var counts counters
	got, err := tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	if err != nil || !reflect.DeepEqual(plain, got) || counts.work().Payloads != 2 || counts.work().PayloadBytes != 2*int64(len(staged)) {
		t.Fatalf("staged duplicate accounting/parity: %+v %v", counts.work(), err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	counts.reset()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, plainErr := tx.Get(cancelled, refs)
	_, observedErr := tx.Get(driver.WithReadWorkObserver(cancelled, &counts), refs)
	if !errors.Is(plainErr, context.Canceled) || !errors.Is(observedErr, context.Canceled) || counts.work().Payloads != 0 {
		t.Fatalf("cancellation must precede closed transaction: %v %v %+v", plainErr, observedErr, counts.work())
	}
	_, plainErr = tx.Get(context.Background(), refs)
	_, observedErr = tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	if !errors.Is(plainErr, driver.ErrTxClosed) || !errors.Is(observedErr, driver.ErrTxClosed) || counts.work().Payloads != 0 {
		t.Fatalf("closed transaction parity: %v %v", plainErr, observedErr)
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, plainErr = tx.Get(context.Background(), refs)
	_, observedErr = tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	var plainFenced, observedFenced driver.ErrFenced
	if !errors.As(plainErr, &plainFenced) || !errors.As(observedErr, &observedFenced) || !reflect.DeepEqual(plainFenced, observedFenced) || counts.work().Payloads != 0 {
		t.Fatalf("lease loss must precede closed transaction: %v %v", plainErr, observedErr)
	}
	t.Log("CP7_BODY_WORK_PARITY staged_duplicates=true cancellation_before_closed=true closed_before_read=true lease_loss_before_closed=true payload_work_on_refusal=0")
}
