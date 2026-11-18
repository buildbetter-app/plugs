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
		checkedSourcePlaneSnapshot: &checkedSourcePlaneSnapshot{Reader: nil},
		AllocatorReader:            snapshotAllocatorReader{},
	}
	if _, ok := any(with).(AllocatorReader); !ok {
		t.Fatal("capable snapshot dropped allocator capability")
	}
}
