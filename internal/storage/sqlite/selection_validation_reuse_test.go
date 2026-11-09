package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type selectionValidationWork struct {
	payloads int64
	bytes    int64
	cancel   context.CancelFunc
}

func (w *selectionValidationWork) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	switch kind {
	case driver.ReadPayloadRecords:
		w.payloads += amount
		if w.cancel != nil {
			w.cancel()
		}
	case driver.ReadPayloadBytes:
		w.bytes += amount
	}
}

func selectionValidationFixture(t *testing.T) (*Store, *transaction, driver.ChangesetHeader, []driver.Record, []driver.DerivedRow) {
	t.Helper()
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	refs := []model.Ref{testRef("validation-source-a"), testRef("validation-source-b")}
	seed, header := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		body := []byte(fmt.Sprintf(`{"id":%q,"type":"task","fields":{"title":"original"}}`, ref.ID))
		if err := seed.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, seed, header)
	r := sourceReader(t, s, nil)
	sources, err := r.Get(ctx, refs)
	_ = r.Close()
	if err != nil || len(sources) != len(refs) {
		t.Fatalf("committed sources: %v %v", sources, err)
	}
	rows := make([]driver.DerivedRow, 64)
	locks := make([]driver.LockKey, len(rows))
	for i := range rows {
		source := sources[i%len(sources)]
		key := driver.DerivedRowKey{Table: driver.DerivedReferenceIndex, Key: fmt.Sprintf("validation-row-%02d", i)}
		value := []byte(`{"links":[]}`)
		rows[i] = driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: header.Incarnation,
			Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
			Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &source.Ref},
				Revision: source.Revision, Present: true, Digest: driver.SelectionDigest(source.Body)},
			Facts: &driver.SelectionFacts{Kind: driver.SelectionLinkFact},
		}}
		locks[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	}
	tx, header := beginSourceTestWithLocks(t, s, lease, locks, true, refs...)
	return s, tx, header, sources, rows
}

func TestSelectionValidationReadsEachSourceOncePerCall(t *testing.T) {
	s, tx, header, sources, rows := selectionValidationFixture(t)
	ctx := context.Background()
	// Replay the actual prior per-annotation observation boundary against this
	// real pinned transaction. The observer counts actual body materializations.
	var scalar selectionValidationWork
	for _, row := range rows {
		state, err := tx.observe(driver.WithReadWorkObserver(ctx, &scalar), row.Index.Source.Key)
		if err != nil || state.Record == nil || driver.SelectionDigest(state.Record.Body) != row.Index.Source.Digest {
			t.Fatalf("scalar source observation: %+v %v", state, err)
		}
	}
	wantBytes := int64(len(sources[0].Body) + len(sources[1].Body))
	if scalar.payloads != 64 || scalar.bytes != 32*wantBytes {
		t.Fatalf("scalar materializations=%+v, want64/%d", scalar, 32*wantBytes)
	}
	for call := 0; call < 2; call++ {
		var work selectionValidationWork
		if err := tx.PutSelectionDerived(driver.WithReadWorkObserver(ctx, &work), driver.DerivedReferenceIndex, nil, rows); err != nil {
			t.Fatal(err)
		}
		if work.payloads != 2 || work.bytes != wantBytes {
			t.Fatalf("call%d materializations=%+v, want2/%d", call, work, wantBytes)
		}
	}
	if err := tx.PutSelectionDerived(driver.WithReadWorkObserver(ctx, nil), driver.DerivedReferenceIndex, nil, rows); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.DerivedRows(ctx, driver.DerivedReferenceIndex, "validation-row-"); err != nil || !reflect.DeepEqual(got, rows) {
		t.Fatalf("observed/nil-observer staged images differ: %v %v", got, err)
	}
	// Commit invokes validation anew; a prior successful call cannot supply
	// its source observations. Count that real validation boundary directly.
	var commitValidation selectionValidationWork
	if err := tx.validateSelectionWrites(driver.WithReadWorkObserver(ctx, &commitValidation), tx.derived); err != nil || commitValidation.payloads != 2 || commitValidation.bytes != wantBytes {
		t.Fatalf("fresh commit validation=%+v: %v", commitValidation, err)
	}
	entry, err := tx.Commit(ctx, header)
	if err != nil || entry.Revision.String() != "2" {
		t.Fatalf("publication: %+v %v", entry, err)
	}
	r := sourceReader(t, s, nil)
	defer r.Close()
	got, err := r.DerivedRows(ctx, driver.DerivedReferenceIndex, "validation-row-")
	if err != nil || !reflect.DeepEqual(got, rows) {
		t.Fatalf("committed annotations changed: %v %v", got, err)
	}
	refs := []model.Ref{sources[0].Ref, sources[1].Ref}
	if got, err := r.Get(ctx, refs); err != nil || !reflect.DeepEqual(got, sources) {
		t.Fatalf("projection publication changed source bytes/tokens: %v %v", got, err)
	}
	t.Logf("real scalar observations64 versus call-local2 on each invocation; bytes%d versus%d, all64 annotations and both sources conserved", scalar.bytes, wantBytes)
}

func TestSelectionValidationReuseRetainsEveryRowRefusal(t *testing.T) {
	for _, name := range []string{"revision", "digest", "presence", "incarnation", "unacquired", "cancel_after_materialization"} {
		t.Run(name, func(t *testing.T) {
			_, tx, _, _, rows := selectionValidationFixture(t)
			ctx := context.Background()
			if err := tx.PutSelectionDerived(ctx, driver.DerivedReferenceIndex, nil, rows); err != nil {
				t.Fatal(err)
			}
			before, err := tx.DerivedRows(ctx, driver.DerivedReferenceIndex, "validation-row-")
			if err != nil {
				t.Fatal(err)
			}
			bad := append([]driver.DerivedRow(nil), rows...)
			bad[63] = bad[63].Clone()
			x := bad[63].Index
			switch name {
			case "revision":
				x.Source.Revision = model.Natural64(0)
			case "digest":
				x.Source.Digest = strings.Repeat("0", 64)
			case "presence":
				x.Source.Present, x.Source.Digest = false, ""
			case "incarnation":
				x.Incarnation = "other-incarnation"
			case "unacquired":
				ref := testRef("validation-unacquired")
				x.Source.Key.Ref = &ref
			case "cancel_after_materialization":
				canceled, cancel := context.WithCancel(ctx)
				defer cancel()
				work := &selectionValidationWork{cancel: cancel}
				ctx = driver.WithReadWorkObserver(canceled, work)
			}
			err = tx.PutSelectionDerived(ctx, driver.DerivedReferenceIndex, nil, bad)
			if name == "cancel_after_materialization" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("reused source suppressed cancellation: %v", err)
				}
			} else if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("one invalid row among64 was admitted: %v", err)
			}
			if after, err := tx.DerivedRows(context.Background(), driver.DerivedReferenceIndex, "validation-row-"); err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("refusal changed staged rows: %v %v", after, err)
			}
		})
	}
}

func TestSelectionValidationReuseRejectsLaterSameTokenSource(t *testing.T) {
	s, tx, header, sources, rows := selectionValidationFixture(t)
	ctx := context.Background()
	if err := tx.PutSelectionDerived(ctx, driver.DerivedReferenceIndex, nil, rows); err != nil {
		t.Fatal(err)
	}
	replacement := sources[0]
	replacement.Body = []byte(fmt.Sprintf(`{"id":%q,"type":"task","fields":{"title":"changed"}}`, replacement.Ref.ID))
	if err := tx.PutGraphRecord(ctx, replacement, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedReferenceIndex, nil, rows); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "source digest changed") {
		t.Fatalf("later staging reused an obsolete source: %v", err)
	}
	header.Changes = []model.ChangeRecord{{Ref: replacement.Ref, Op: model.OpPatch, FromRevision: replacement.Revision}}
	if _, err := tx.Commit(ctx, header); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "source digest changed") {
		t.Fatalf("Commit reused an obsolete source: %v", err)
	}
	_ = tx.Close()
	r := sourceReader(t, s, nil)
	defer r.Close()
	if r.Snapshot().At.String() != "1" {
		t.Fatal("refused publication advanced Head")
	}
	if got, err := r.Get(ctx, []model.Ref{sources[0].Ref, sources[1].Ref}); err != nil || !reflect.DeepEqual(got, sources) {
		t.Fatalf("refused publication changed durable sources: %v %v", got, err)
	}
	if got, err := r.DerivedRows(ctx, driver.DerivedReferenceIndex, "validation-row-"); err != nil || len(got) != 0 {
		t.Fatalf("refused publication left durable annotations: %v %v", got, err)
	}
}
