// Package readyorder defines the public Ready order shared by readers and
// optional index certificates. It does not certify membership or freshness.
package readyorder

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

const RecentWindow = 48 * time.Hour

type Key struct {
	Bucket, Rank int
	CreatedAt    time.Time
}

// Classify uses the caller's observed clock. Exactly 48 hours old is recent;
// older items share rank 999 regardless of their original priority.
func Classify(priority int, createdAt, at time.Time) Key {
	key := Key{Rank: priority, CreatedAt: createdAt}
	if createdAt.Before(at.Add(-RecentWindow)) {
		key.Bucket, key.Rank = 1, 999
	}
	return key
}

// Compare orders bucket, rank, creation instant, then bytewise ID. Persisted
// callers supply their already validated rank, including any permitted lag.
func Compare(a Key, aID string, b Key, bID string) int {
	if a.Bucket != b.Bucket {
		if a.Bucket < b.Bucket {
			return -1
		}
		return 1
	}
	if a.Rank != b.Rank {
		if a.Rank < b.Rank {
			return -1
		}
		return 1
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		return 1
	}
	return strings.Compare(aID, bID)
}

// EqualGroup is the exact boundary within which ClaimNext may rotate.
func EqualGroup(a, b Key) bool {
	return a.Bucket == b.Bucket && a.Rank == b.Rank && a.CreatedAt.Equal(b.CreatedAt)
}

// Pivot retains the public seed hash of the group's first original priority.
// In the old bucket that priority can differ from the group's effective rank.
func Pivot(seed string, originalPriority int64, count uint64) uint64 {
	if seed == "" || count < 2 {
		return 0
	}
	hash := sha256.Sum256([]byte(seed + "\x00" + strconv.FormatInt(originalPriority, 10)))
	return binary.BigEndian.Uint64(hash[:8]) % count
}
