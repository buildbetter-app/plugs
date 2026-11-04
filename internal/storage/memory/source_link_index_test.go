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

func sourceLinkTestRecord(id string, revision uint64, body []byte, links ...sourceLink) sourceLinkSource {
	return sourceLinkSource{record: driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(revision), Body: body}, links: links}
}

func sourceLinkTestFact(kind, target string) sourceLink {
	return sourceLink{kind: kind, target: model.RefByID(target, model.Natural64(0))}
}

func TestSourceLinkIndexCoverageIsIndependentAndLegacyWritesLeaveGap(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	inc := s.state.meta.Incarnation
	parentCoverage, err := s.state.hierarchy.certify(newHierarchyRoot(0))
	if err != nil {
		t.Fatal(err)
	}
	s.state.hierarchy = parentCoverage
	if root, err := s.state.sourceLinks.rootAt(inc, 0, 0); root != nil || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("Parent-only coverage certified outgoing links: root=%v err=%v", root, err)
	}
	links, err := s.state.sourceLinks.certify(newSourceLinkRoot(0))
	if err != nil {
		t.Fatal(err)
	}
	s.state.sourceLinks = links
	lease, err := s.Lease(ctx, "source-link-state", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: model.RefByID("pl-legacy", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"links":[]}`)}})
	if err != nil {
		// The later publication layer may refuse this write before staging.
		if !errors.Is(err, driver.ErrUnsupported) || !s.state.meta.Head.IsZero() || len(s.state.records) != 0 {
			t.Fatalf("legacy refusal changed source state or had an unexpected cause: %v", err)
		}
		return
	}
	at, _ := entry.Revision.Uint64()
	if root, err := s.state.sourceLinks.rootAt(inc, 0, at); root != nil || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("untyped writer left certified stale facts: root=%v err=%v", root, err)
	}
}

func TestSourceLinkIndexImmutableCompleteImagesAndIncomingKeys(t *testing.T) {
	zero := newSourceLinkRoot(0)
	body := []byte("opaque\x00\xff\n")
	facts := []sourceLink{sourceLinkTestFact("parent-child", "pl-c"), sourceLinkTestFact("blocks", "pl-p"), sourceLinkTestFact("custom\x00kind", "pl-c")}
	first, err := zero.apply(1, []sourceLinkSource{
		sourceLinkTestRecord("pl-x", 1, body, facts...),
		sourceLinkTestRecord("pl-a", 1, []byte("a"), sourceLinkTestFact("blocks", "pl-p")),
		sourceLinkTestRecord("pl-empty", 1, nil),
		sourceLinkTestRecord("pl-unrelated", 1, []byte("unrelated"), sourceLinkTestFact("related", "pl-q")),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = '!'
	facts[0].kind = "mutated"
	x := hierarchyTestKey(t, "pl-x")
	empty := hierarchyTestKey(t, "pl-empty")
	if !bytes.Equal(first.sources[x].record.Body, []byte("opaque\x00\xff\n")) || first.sources[x].links[0].kind != "parent-child" || len(zero.sources) != 0 {
		t.Fatal("exact source image, metadata list or original root was aliased")
	}
	if _, known := first.sources[empty]; !known || len(first.sources[empty].links) != 0 {
		t.Fatal("explicit empty outgoing set was conflated with missing metadata")
	}
	block, _ := sourceLinkKey(sourceLinkTestFact("blocks", "pl-p"))
	legacy, _ := sourceLinkKey(sourceLinkTestFact("parent-child", "pl-c"))
	custom, _ := sourceLinkKey(sourceLinkTestFact("custom\x00kind", "pl-c"))
	if !reflect.DeepEqual(first.incoming[block], []string{hierarchyTestKey(t, "pl-a"), x}) || !reflect.DeepEqual(first.incoming[legacy], []string{x}) || !reflect.DeepEqual(first.incoming[custom], []string{x}) {
		t.Fatalf("incoming sorted source-key sets changed: %+v", first.incoming)
	}
	second, err := first.apply(2, []sourceLinkSource{sourceLinkTestRecord("pl-x", 2, []byte(" replacement \n"), sourceLinkTestFact("waits-for", "pl-z"), sourceLinkTestFact("waits-for", "pl-z"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waits, _ := sourceLinkKey(sourceLinkTestFact("waits-for", "pl-z"))
	if len(second.incoming[legacy]) != 0 || len(second.incoming[custom]) != 0 || len(second.incoming[block]) != 1 || !reflect.DeepEqual(second.incoming[waits], []string{x}) || string(second.sources[x].record.Body) != " replacement \n" {
		t.Fatal("complete outgoing replacement failed or duplicate facts duplicated a source")
	}
	if len(first.incoming[legacy]) != 1 || len(first.incoming[block]) != 2 || first.sources[x].links[0].kind != "parent-child" {
		t.Fatal("replacement changed a pinned root")
	}
	unrelated, _ := sourceLinkKey(sourceLinkTestFact("related", "pl-q"))
	if &first.incoming[unrelated][0] != &second.incoming[unrelated][0] {
		t.Fatal("unaffected incoming bucket was copied")
	}
	third, err := second.apply(3, []sourceLinkSource{sourceLinkTestRecord("pl-x", 3, []byte{})}, nil)
	if err != nil || len(third.incoming[waits]) != 0 || third.sources[x].record.Body == nil {
		t.Fatalf("explicit empty replacement: %+v %v", third, err)
	}
	fourth, err := third.apply(4, nil, []string{x})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fourth.sources[x]; ok {
		t.Fatal("deleted source retained complete-image metadata")
	}
	index := newSourceLinkIndex("links")
	for _, root := range []*sourceLinkRoot{zero, first, second, third, fourth} {
		index, err = index.certify(root)
		if err != nil {
			t.Fatal(err)
		}
	}
	for at, want := range []*sourceLinkRoot{zero, first, second, third, fourth} {
		if got, err := index.rootAt("links", 0, uint64(at)); err != nil || got != want {
			t.Fatalf("historical outgoing root %d: got=%p want=%p err=%v", at, got, want, err)
		}
	}
}

func TestSourceLinkIndexSnapshotBoundsForksAndInvalidMetadata(t *testing.T) {
	s := New()
	defer s.Close()
	root := newSourceLinkRoot(math.MaxUint64 - 1)
	index, err := s.state.sourceLinks.certify(root)
	if err != nil {
		t.Fatal(err)
	}
	s.state.sourceLinks = index
	next := s.state.fork()
	last, err := root.apply(math.MaxUint64, []sourceLinkSource{sourceLinkTestRecord("pl-x", math.MaxUint64, []byte("x"), sourceLinkTestFact("blocks", "pl-p"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	next.sourceLinks, err = next.sourceLinks.certify(last)
	if err != nil || len(index.roots) != 1 || index.through != math.MaxUint64-1 || len(root.sources) != 0 {
		t.Fatalf("fork aliased coverage/state: %+v %v", next.sourceLinks, err)
	}
	for _, probe := range []struct {
		inc        string
		branch, at uint64
	}{{index.incarnation, 0, 0}, {"restored", 0, math.MaxUint64}, {index.incarnation, 1, math.MaxUint64}} {
		if got, err := next.sourceLinks.rootAt(probe.inc, probe.branch, probe.at); got != nil || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("uncovered outgoing snapshot: root=%v err=%v", got, err)
		}
	}
	if _, err := next.sourceLinks.certify(newSourceLinkRoot(0)); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("outgoing coverage wrapped: %v", err)
	}
	for _, bad := range []sourceLink{{kind: "", target: model.RefByID("pl-p", model.Natural64(0))}, {kind: "blocks", target: model.RefByKey("alias")}, {kind: "blocks", target: model.RefByID("pl-p", model.Natural64(1))}, {kind: "\xff", target: model.RefByID("pl-p", model.Natural64(0))}} {
		if got, err := root.apply(math.MaxUint64, []sourceLinkSource{sourceLinkTestRecord("pl-invalid", math.MaxUint64, []byte("opaque"), bad)}, nil); got != nil || err == nil {
			t.Fatalf("invalid typed fact accepted: root=%v err=%v", got, err)
		}
	}
}
