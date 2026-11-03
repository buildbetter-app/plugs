package memory

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func hierarchyTestKey(t *testing.T, id string) string {
	t.Helper()
	key, err := recordKey(model.RefByID(id, model.Natural64(0)))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func hierarchyTestSource(t *testing.T, id, parent string, revision uint64, body []byte) hierarchySource {
	t.Helper()
	source := hierarchySource{record: driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(revision), Body: body}}
	if parent != "" {
		source.parentKey = hierarchyTestKey(t, parent)
	}
	return source
}

func TestHierarchyIndexStartsUnbuiltAndLegacyWritesDoNotCertify(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	incarnation := s.state.meta.Incarnation
	if root, err := s.state.hierarchy.rootAt(incarnation, 0, 0); root != nil || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("fresh unbuilt index: root=%v err=%v", root, err)
	}
	lease, err := s.Lease(ctx, "hierarchy-state", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: model.RefByID("pl-legacy", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"parent":{"id":"pl-parent"}}`)}})
	if err != nil {
		t.Fatal(err)
	}
	at, _ := entry.Revision.Uint64()
	if len(s.state.records) != 1 {
		t.Fatal("legacy source was not published")
	}
	if root, err := s.state.hierarchy.rootAt(incarnation, 0, at); root != nil || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("legacy writer certified missing parent metadata: root=%v err=%v", root, err)
	}
}

func TestHierarchyIndexImmutableImagesMovesAndHistory(t *testing.T) {
	incarnation := "hierarchy-state"
	index := newHierarchyIndex(incarnation)
	zero := newHierarchyRoot(0)
	index, err := index.certify(zero)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte(" opaque\x00\xff body\n")
	first, err := zero.apply(1, []hierarchySource{
		hierarchyTestSource(t, "pl-z", "pl-p", 1, []byte("z")),
		hierarchyTestSource(t, "pl-a:child/one", "pl-p", 1, input),
		hierarchyTestSource(t, "pl-unrelated", "pl-q", 1, []byte("unrelated")),
		hierarchyTestSource(t, "pl-root", "", 1, nil),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = '!'
	p, q := hierarchyTestKey(t, "pl-p"), hierarchyTestKey(t, "pl-q")
	child := hierarchyTestKey(t, "pl-a:child/one")
	if !bytes.Equal(first.sources[child].record.Body, []byte(" opaque\x00\xff body\n")) || len(zero.sources) != 0 {
		t.Fatal("input body or original empty root was aliased")
	}
	if len(first.children[p]) != 2 || first.children[p][0].record.Ref.ID != "pl-a:child/one" {
		t.Fatalf("parent bucket is not sorted: %+v", first.children[p])
	}
	rootKey := hierarchyTestKey(t, "pl-root")
	if source, ok := first.sources[rootKey]; !ok || source.parentKey != "" || source.record.Body != nil || len(first.children[""]) != 0 {
		t.Fatal("explicit root was conflated with absent metadata")
	}
	index, err = index.certify(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := first.apply(2, []hierarchySource{
		hierarchyTestSource(t, "pl-a:child/one", "pl-p", 2, []byte(" exact replacement \n")),
		hierarchyTestSource(t, "pl-z", "pl-q", 2, []byte("moved")),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.children[p]) != 1 || len(second.children[q]) != 2 || !bytes.Equal(second.sources[child].record.Body, []byte(" exact replacement \n")) {
		t.Fatal("same-parent image replacement or reparent failed")
	}
	if len(first.children[p]) != 2 || len(first.children[q]) != 1 || string(first.sources[child].record.Body) != " opaque\x00\xff body\n" {
		t.Fatal("reparent mutated a pinned root")
	}
	index, err = index.certify(second)
	if err != nil {
		t.Fatal(err)
	}
	third, err := second.apply(3, nil, []string{child, hierarchyTestKey(t, "pl-z")})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := third.children[p]; ok || len(third.children[q]) != 1 {
		t.Fatal("delete retained a source edge or empty bucket")
	}
	index, err = index.certify(third)
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := third.apply(4, []hierarchySource{hierarchyTestSource(t, "pl-a:child/one", "", 4, []byte{})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if &third.children[q][0] != &fourth.children[q][0] {
		t.Fatal("untouched immutable parent bucket was copied")
	}
	index, err = index.certify(fourth)
	if err != nil {
		t.Fatal(err)
	}
	for revision, want := range []*hierarchyRoot{zero, first, second, third, fourth} {
		got, err := index.rootAt(incarnation, 0, uint64(revision))
		if err != nil || got != want {
			t.Fatalf("historical root %d: got=%p want=%p err=%v", revision, got, want, err)
		}
	}
	if value := fourth.sources[child].record.Body; value == nil || len(value) != 0 {
		t.Fatal("empty exact body was conflated with nil")
	}
}

func TestHierarchyIndexCoverageBoundsAndForkIsolation(t *testing.T) {
	s := New()
	defer s.Close()
	old := s.state
	index, err := old.hierarchy.certify(newHierarchyRoot(math.MaxUint64 - 1))
	if err != nil {
		t.Fatal(err)
	}
	old.hierarchy = index
	next := old.fork()
	root, err := index.roots[0].apply(math.MaxUint64, []hierarchySource{hierarchyTestSource(t, "pl-max", "pl-p", math.MaxUint64, []byte("max"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	next.hierarchy, err = next.hierarchy.certify(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(old.hierarchy.roots) != 1 || old.hierarchy.through != math.MaxUint64-1 || len(old.hierarchy.roots[0].sources) != 0 {
		t.Fatal("fork changed the published index or root slice")
	}
	if _, err := next.hierarchy.rootAt(old.meta.Incarnation, 0, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		incarnation string
		branch, at  uint64
	}{{old.meta.Incarnation, 0, 0}, {old.meta.Incarnation, 1, math.MaxUint64}, {"restored", 0, math.MaxUint64}} {
		if got, err := next.hierarchy.rootAt(probe.incarnation, probe.branch, probe.at); got != nil || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("uncovered snapshot returned root=%v err=%v", got, err)
		}
	}
	if _, err := next.hierarchy.certify(newHierarchyRoot(0)); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("coverage wrapped uint64: %v", err)
	}
	initial, _ := newHierarchyIndex("gap").certify(newHierarchyRoot(10))
	if _, err := initial.certify(newHierarchyRoot(12)); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("coverage skipped an unindexed publication: %v", err)
	}
	if got, err := initial.rootAt("gap", 0, 11); got != nil || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("coverage exceeded certified head: %v %v", got, err)
	}
}

func TestHierarchyIndexRejectsInvalidImagesWithoutChangingRoot(t *testing.T) {
	root := newHierarchyRoot(0)
	valid := hierarchyTestSource(t, "pl-child", "pl-parent", 1, []byte("opaque"))
	for _, change := range []func(*hierarchySource){
		func(s *hierarchySource) { s.record.Ref.Branch = model.Natural64(1) },
		func(s *hierarchySource) { s.record.Ref = model.RefByKey("alias") },
		func(s *hierarchySource) { s.record.Revision = model.Natural64(2) },
		func(s *hierarchySource) { s.record.Kind = "other" },
		func(s *hierarchySource) { s.parentKey = hierarchyTestKey(t, "pl-child") },
	} {
		invalid := valid
		change(&invalid)
		if got, err := root.apply(1, []hierarchySource{valid, invalid}, nil); err == nil || got != nil {
			t.Fatalf("invalid source accepted: %+v %v", got, err)
		}
		if !reflect.DeepEqual(root, newHierarchyRoot(0)) {
			t.Fatal("rejected update mutated original root")
		}
	}
	got, err := root.apply(1, []hierarchySource{valid, valid}, nil)
	if err != nil || len(got.sources) != 1 || len(got.children[valid.parentKey]) != 1 {
		t.Fatalf("replacement duplicated child: %+v %v", got, err)
	}
}
