package conformance

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type selectionWork struct {
	mu     sync.Mutex
	counts map[driver.ReadWorkKind]int64
}

func (w *selectionWork) ObserveReadWork(kind driver.ReadWorkKind, n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.counts == nil {
		w.counts = make(map[driver.ReadWorkKind]int64)
	}
	w.counts[kind] += n
}

func (w *selectionWork) count(kind driver.ReadWorkKind) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counts[kind]
}

// The same concrete reader and transaction execute both calls. No wrapper can
// hide Select, fresh committed existence, staged publication or lock semantics.
func selectionExecutedWork(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	images := []selectionImage{
		selectionImageOf("a", "issue", "P", "a", 0),
		selectionImageOf("b", "issue", "P", "b", 0),
		selectionImageOf("c", "issue", "P", "c", 0),
	}
	selectionSeed(t, ctx, s, lease, images, nil)
	r, base := selectionOpen(t, ctx, s)
	selector := r.(driver.SelectionReader)
	for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
		t.Run(string(mode), func(t *testing.T) {
			p := base
			p.Mode, p.Page = mode, nil
			if mode == driver.SelectionPageMode {
				p.Page = &driver.SelectionPage{Limit: 2}
			}
			if mode != driver.SelectionCountMode {
				p.GroupBy = driver.SelectionGroupType
			}
			plain, err := selector.Select(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			var work selectionWork
			got, err := selector.Select(driver.WithReadWorkObserver(ctx, &work), p)
			if err != nil || !reflect.DeepEqual(got, plain) || got.Total != 3 {
				t.Fatalf("observed selection changed complete result: %+v %v", got, err)
			}
			if work.count(driver.ReadSelectionCalls) != 1 || work.count(driver.ReadFreshSelectionCalls) != 0 {
				t.Fatalf("selection entry escaped accounting: %+v", work.counts)
			}
			// Memory has no SQL statement/control row. SQL exposes one decoded
			// aggregate control even for count/summary with zero emitted keys.
			statements := work.count(driver.ReadSelectionStatements)
			if statements > 1 || work.count(driver.ReadSelectionControls) != statements {
				t.Fatalf("statement/control work differs: %+v", work.counts)
			}
			if mode != driver.SelectionPageMode && len(got.Keys) != 0 {
				t.Fatal("aggregate materialized an output key page")
			}
		})
	}
	capPlan := base
	capPlan.Cap = 2
	var capWork selectionWork
	capResult, capErr := selector.Select(driver.WithReadWorkObserver(ctx, &capWork), capPlan)
	var cap driver.ErrSelectionCap
	if !errors.As(capErr, &cap) || !reflect.DeepEqual(capResult, driver.SelectionResult{}) || capWork.count(driver.ReadSelectionCalls) != 1 {
		t.Fatalf("cap probe hid work or published partial result: %+v %v %+v", capResult, capErr, capWork.counts)
	}
	// Staged content and fresh committed EXISTS must remain distinct, including
	// when the caller uses the transaction's inherited Reader interface.
	images[0].deleted, images[1].deleted, images[2].deleted = true, true, true
	staged := selectionImageOf("staged", "issue", "P", "new", 0)
	tx, _ := selectionStage(t, ctx, s, lease, append(images, staged), nil)
	defer tx.Rollback(ctx)
	if _, ok := tx.(driver.SelectionIndexWriter); !ok {
		t.Fatal("typed transaction capability hidden")
	}
	fresh, ok := tx.(driver.FreshSelectionExistsReader)
	if !ok {
		t.Fatal("fresh committed transaction capability hidden")
	}
	var txWork selectionWork
	observed := driver.WithReadWorkObserver(ctx, &txWork)
	got := selectionSelect(t, observed, tx, selectionPlan(tx.Snapshot()))
	selectionWant(t, got, 1, "staged")
	freshPlan := base
	freshPlan.Mode, freshPlan.Page = driver.SelectionCountMode, nil
	plain, err := fresh.FreshSelectionExists(ctx, freshPlan)
	if err != nil || !plain {
		t.Fatalf("committed control: %t %v", plain, err)
	}
	yes, err := fresh.FreshSelectionExists(observed, freshPlan)
	if err != nil || yes != plain || txWork.count(driver.ReadSelectionCalls) != 1 || txWork.count(driver.ReadFreshSelectionCalls) != 1 {
		t.Fatalf("observed transaction lost committed/staged separation: %t %v %+v", yes, err, txWork.counts)
	}
	freshPlan.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []driver.Ref{staged.bead.Ref}}
	if yes, err := fresh.FreshSelectionExists(observed, freshPlan); err != nil || yes {
		t.Fatalf("staged candidate became committed existence: %t %v", yes, err)
	}
	if txWork.count(driver.ReadFreshSelectionCalls) != 2 {
		t.Fatal("empty existence work omitted")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("SELECTION_EXECUTED_WORK reader_and_tx_concrete=true staged_vs_committed=true result_statements=%d control_rows=%d executor_inputs=UNMEASURED", txWork.count(driver.ReadSelectionStatements), txWork.count(driver.ReadSelectionControls))
}
