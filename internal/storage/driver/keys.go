package driver

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// EncodeLockKey is the common engine-neutral identity and ordering encoding.
// Compare the returned strings bytewise: record < read_fact < sentinel <
// derived, then the canonical encoded key. Ref branches normalize through
// model.Natural, so e.g. branch 1 and 1.0 denote the same lock.
func EncodeLockKey(key LockKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	return lockKeySortKey(key), nil
}

// ValidateLockKeys checks the exact order used by Lock and Observe, without
// inventing a lock mode for snapshot observations or commit declarations.
func ValidateLockKeys(keys []LockKey) error {
	previous := ""
	for i, key := range keys {
		current, err := EncodeLockKey(key)
		if err != nil {
			return fmt.Errorf("lock key %d: %w", i, err)
		}
		if i > 0 && current <= previous {
			return fmt.Errorf("lock set is unsorted or contains a duplicate key at %d", i)
		}
		previous = current
	}
	return nil
}

// ValidateHeaderLocks enforces G3's exact declaration of the acquired set.
// acquired contains only successfully acquired locks, excluding skipped keys.
func ValidateHeaderLocks(declared []LockKey, acquired LockSet) error {
	if err := ValidateLockKeys(declared); err != nil {
		return err
	}
	if err := ValidateLockSet(acquired); err != nil {
		return err
	}
	if len(declared) != len(acquired) {
		return fmt.Errorf("commit lock declaration does not match acquired set")
	}
	for i := range declared {
		if lockKeySortKey(declared[i]) != lockKeySortKey(acquired[i].Key) {
			return fmt.Errorf("commit lock declaration differs at %d", i)
		}
	}
	return nil
}

// PrincipalKey encodes effective identity for the G4 idempotency scope.
// Sponsor/caveats are authorization context and do not change that identity.
// Principal kinds are closed and contain no colon; IDs remain lossless.
func PrincipalKey(principal model.PrincipalRef) (string, error) {
	if _, err := json.Marshal(principal); err != nil {
		return "", fmt.Errorf("idempotency principal: %w", err)
	}
	return principal.Kind + ":" + principal.ID, nil
}

func ValidateCheckpointKey(key string) error {
	if key == "" || strings.TrimSpace(key) != key || strings.ContainsRune(key, '\x00') {
		return fmt.Errorf("checkpoint key is not canonical")
	}
	return nil
}
