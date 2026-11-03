package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

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
