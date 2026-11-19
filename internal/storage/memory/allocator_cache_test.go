package memory

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"testing"
)

func TestAllocatorCacheCrossEngineBoundariesAndEmptyRestore(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "allocator-cache", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	sourceactivation.AllocatorCache(t, s, lease)
}
