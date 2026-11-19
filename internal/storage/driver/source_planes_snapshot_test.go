package driver

import (
	"context"
	"errors"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"testing"
)

// A snapshot must expose exactly the optional capabilities of its pinned reader.
// These embedded interfaces are used only for type assertions, never invoked.
func TestSourcePlaneSnapshotPreservesOptionalReaderCapabilities(t *testing.T) {
	probe := snapshotCapabilityProbe{}
	cases := []struct {
		name  string
		inner Reader
	}{
		{"none", struct{ Reader }{}},
		{"allocator", struct {
			Reader
			AllocatorReader
		}{AllocatorReader: probe}},
		{"hierarchy", struct {
			Reader
			HierarchyChildrenReader
		}{HierarchyChildrenReader: probe}},
		{"inverse", struct {
			Reader
			InverseSourceLinksReader
		}{InverseSourceLinksReader: probe}},
		{"allocator-hierarchy", struct {
			Reader
			AllocatorReader
			HierarchyChildrenReader
		}{AllocatorReader: probe, HierarchyChildrenReader: probe}},
		{"allocator-inverse", struct {
			Reader
			AllocatorReader
			InverseSourceLinksReader
		}{AllocatorReader: probe, InverseSourceLinksReader: probe}},
		{"hierarchy-inverse", struct {
			Reader
			HierarchyChildrenReader
			InverseSourceLinksReader
		}{HierarchyChildrenReader: probe, InverseSourceLinksReader: probe}},
		{"all", struct {
			Reader
			AllocatorReader
			HierarchyChildrenReader
			InverseSourceLinksReader
		}{AllocatorReader: probe, HierarchyChildrenReader: probe, InverseSourceLinksReader: probe}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := newCheckedSourcePlaneSnapshot(tc.inner, nil)
			_, wantAllocator := tc.inner.(AllocatorReader)
			_, gotAllocator := snapshot.(AllocatorReader)
			_, wantHierarchy := tc.inner.(HierarchyChildrenReader)
			_, gotHierarchy := snapshot.(HierarchyChildrenReader)
			_, wantInverse := tc.inner.(InverseSourceLinksReader)
			_, gotInverse := snapshot.(InverseSourceLinksReader)
			if gotAllocator != wantAllocator || gotHierarchy != wantHierarchy || gotInverse != wantInverse {
				t.Errorf("capabilities allocator/hierarchy/inverse: got %v/%v/%v, want %v/%v/%v", gotAllocator, gotHierarchy, gotInverse, wantAllocator, wantHierarchy, wantInverse)
			}
			if v, ok := snapshot.(AllocatorReader); ok {
				if _, err := v.LoadAllocator(context.Background(), "pinned"); !errors.Is(err, ErrConflict) {
					t.Fatal("allocator forwarding", err)
				}
			}
			if v, ok := snapshot.(HierarchyChildrenReader); ok {
				if _, _, err := v.HierarchyChildren(context.Background(), nil, 7); !errors.Is(err, ErrConflict) {
					t.Fatal("hierarchy forwarding", err)
				}
			}
			if v, ok := snapshot.(InverseSourceLinksReader); ok {
				if _, _, err := v.InverseSourceLinks(context.Background(), nil, nil, nil, 7); !errors.Is(err, ErrConflict) {
					t.Fatal("inverse forwarding", err)
				}
			}

		})
	}
}

type snapshotCapabilityProbe struct{}

func (snapshotCapabilityProbe) LoadAllocator(context.Context, string) (OfflineAllocator, error) {
	return OfflineAllocator{}, ErrConflict
}
func (snapshotCapabilityProbe) HierarchyChildren(context.Context, []model.Ref, int) ([]Record, bool, error) {
	return nil, false, ErrConflict
}
func (snapshotCapabilityProbe) InverseSourceLinks(context.Context, []model.Ref, []IncomingLinkKind, []model.Ref, int) ([]Record, bool, error) {
	return nil, false, ErrConflict
}
