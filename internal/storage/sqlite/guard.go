package sqlite

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// 1024 stripes leave room for small concurrent lock sets while bounding each
// transaction far below Linux's default VMA limit, even for a large restore.
// The unreleased per-key hashed-file layout is replaced by guard-stripe-NNNN;
// every process opening a store must use this same stripe count and hash.
const guardStripeCount = 1024

type guardStripeRequest struct {
	stripe     int
	share      bool
	skipLocked bool
}

func guardStripe(key driver.LockKey) int {
	hash := sha256.Sum256([]byte("guard:" + keyID(key)))
	return int(binary.BigEndian.Uint16(hash[:2])) % guardStripeCount
}

func guardStripePath(lockDir string, stripe int) string {
	return filepath.Join(lockDir, fmt.Sprintf("guard-stripe-%04d", stripe))
}

// planGuardStripes merges modes before acquiring anything: an exclusive key
// promotes its whole stripe, and only a stripe containing solely skipLocked
// requests may be omitted. Collisions add contention, never weaker locking.
// Stripe order, not key order, is the cross-process acquisition order. The
// per-request indexes let Lock return the selected keys in their input order.
func planGuardStripes(requests []driver.LockRequest) ([]guardStripeRequest, []int) {
	var modes [guardStripeCount]struct {
		used      bool
		exclusive bool
		required  bool
	}
	indexes := make([]int, len(requests))
	count := 0
	for i, request := range requests {
		stripe := guardStripe(request.Key)
		indexes[i] = stripe
		mode := &modes[stripe]
		if !mode.used {
			mode.used = true
			count++
		}
		mode.exclusive = mode.exclusive || request.Mode != driver.LockShare
		mode.required = mode.required || request.Mode != driver.LockSkipLocked
	}
	stripes := make([]guardStripeRequest, 0, count)
	for stripe, mode := range modes {
		if mode.used {
			stripes = append(stripes, guardStripeRequest{stripe: stripe, share: !mode.exclusive, skipLocked: !mode.required})
		}
	}
	return stripes, indexes
}
