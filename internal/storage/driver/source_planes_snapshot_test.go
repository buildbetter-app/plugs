package driver

import (
	"context"
	"testing"
)

type snapshotAllocatorReader struct{}

func (snapshotAllocatorReader) LoadAllocator(context.Context, string) (OfflineAllocator, error) {
	return OfflineAllocator{}, nil
}

func TestCheckedSourcePlaneSnapshotPreservesOptionalAllocatorCapabilityConditionally(t *testing.T) {
	without := &checkedSourcePlaneSnapshot{Reader: nil}
	if _, ok := any(without).(AllocatorReader); ok {
		t.Fatal("incapable snapshot falsely advertises allocator capability")
	}
	with := &allocatorSourcePlaneSnapshot{
		SourcePlaneSnapshot: &checkedSourcePlaneSnapshot{Reader: nil},
		AllocatorReader:     snapshotAllocatorReader{},
	}
	if _, ok := any(with).(AllocatorReader); !ok {
		t.Fatal("capable snapshot dropped allocator capability")
	}
}

func TestOptionalSnapshotWrappersDoNotAdvertiseAbsentCapabilities(t *testing.T) {
	base := SourcePlaneSnapshot(&checkedSourcePlaneSnapshot{Reader: nil})
	if _, ok := base.(HierarchyChildrenReader); ok {
		t.Fatal("base snapshot falsely advertises hierarchy capability")
	}
	if _, ok := base.(InverseSourceLinksReader); ok {
		t.Fatal("base snapshot falsely advertises inverse-links capability")
	}
	withParents := SourcePlaneSnapshot(&hierarchySourcePlaneSnapshot{SourcePlaneSnapshot: base, HierarchyChildrenReader: nil})
	if _, ok := withParents.(HierarchyChildrenReader); !ok {
		t.Fatal("hierarchy wrapper dropped capability")
	}
}
