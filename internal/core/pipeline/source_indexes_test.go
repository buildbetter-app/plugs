package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type sourceIndexResultStore struct {
	driver.Store
	result error
}

func (s sourceIndexResultStore) BuildGraphSourceIndexes(context.Context, driver.ActiveLease, driver.GraphSourceDecoder, ...driver.GraphSourceProjection) error {
	return s.result
}

func TestSourceIndexStartupRequiresDirectGraphCompletion(t *testing.T) {
	store := memory.New()
	defer store.Close()
	safe := driver.NewSelectionBackfillRefused(fmt.Errorf("selection: %w", driver.ErrSelectionUnavailable))
	for _, test := range []struct {
		name     string
		err      error
		optional bool
	}{
		{"success", nil, true},
		{"completed_unavailable", safe, true},
		{"completed_unsupported", driver.NewSelectionBackfillRefused(driver.ErrUnsupported), true},
		{"raw_graph_unavailable", driver.ErrSelectionUnavailable, false},
		{"raw_graph_unsupported", driver.ErrUnsupported, false},
		{"mixed_cancellation", errors.Join(driver.ErrSelectionUnavailable, context.Canceled), false},
		{"mixed_fence", errors.Join(driver.ErrSelectionUnavailable, driver.ErrFenced{}), false},
		{"wrapped_completion", fmt.Errorf("unconfirmed outer operation: %w", safe), false},
		{"joined_completion", errors.Join(safe, errors.New("SQL cleanup failed")), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := BuildSourceIndexes(context.Background(), sourceIndexResultStore{Store: store, result: test.err}, nil)
			if test.optional {
				if err != nil {
					t.Fatalf("completed graph lost optional fallback: %v", err)
				}
			} else if err != test.err {
				t.Fatalf("public startup suppressed or changed a fatal result: want=%v got=%v", test.err, err)
			}
		})
	}
}

func TestSourceIndexStartupRejectsMismatchedCoreDocument(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "invalid-legacy-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	ref := model.RefByID("pl-stored", model.Natural64(0))
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-not-the-record","type":"issue","fields":{}}`)}}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := BuildSourceIndexes(ctx, store, lease); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("malformed legacy document was certified: %v", err)
	}
	after, err := store.Meta(ctx)
	if err != nil || before != after {
		t.Fatalf("failed activation changed metadata: before=%+v after=%+v err=%v", before, after, err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed core decode published parent coverage: %v", err)
	}
	if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed core decode published link coverage: %v", err)
	}
}
