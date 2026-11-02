package driver

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const parentCounterNamespace = "parent-counter/"

// ParentCounter is internal continuation state, not a project allocator mode.
// Its identity includes the store incarnation and the canonical parent ID.
// It is independent of compactable record/version history.
type ParentCounter struct {
	Incarnation string `json:"incarnation"`
	Parent      string `json:"parent"`
	LastChild   uint64 `json:"last_child"`
}

func ParentCounterKey(incarnation, parent string) (string, error) {
	if !utf8.ValidString(incarnation) || !utf8.ValidString(parent) {
		return "", fmt.Errorf("parent counter identity is not UTF-8")
	}
	if err := canonicalAllocatorPart("incarnation", incarnation); err != nil {
		return "", err
	}
	if _, err := NewBeadSentinel(parent); err != nil {
		return "", err
	}
	// Normal project prefixes forbid slash. Length-independent hex components
	// and the separator keep both this namespace and the identity injective.
	return parentCounterNamespace + hex.EncodeToString([]byte(incarnation)) + "/" + hex.EncodeToString([]byte(parent)), nil
}

func ParentCounterLock(incarnation, parent string) (LockKey, error) {
	key, err := ParentCounterKey(incarnation, parent)
	if err != nil {
		return LockKey{}, err
	}
	fact, err := NewReadFact(key)
	return LockKey{Kind: LockReadFact, Fact: fact}, err
}

func (value ParentCounter) Validate(incarnation, parent string) error {
	if _, err := ParentCounterKey(incarnation, parent); err != nil {
		return err
	}
	if value.Incarnation != incarnation || value.Parent != parent {
		return fmt.Errorf("parent counter differs from its scoped identity")
	}
	return nil
}

func (value ParentCounter) Next(observed uint64) (string, error) {
	if err := value.Validate(value.Incarnation, value.Parent); err != nil {
		return "", err
	}
	current := value.LastChild
	if observed > current {
		current = observed
	}
	if current == ^uint64(0) {
		return "", fmt.Errorf("%w: child counter exhausted", ErrUnsupported)
	}
	return fmt.Sprintf("%s.%d", value.Parent, current+1), nil
}

func (value ParentCounter) Consume(expected string) (ParentCounter, error) {
	if err := value.Validate(value.Incarnation, value.Parent); err != nil {
		return ParentCounter{}, err
	}
	n, err := ChildNumber(value.Parent, expected)
	if err != nil {
		return ParentCounter{}, err
	}
	if n <= value.LastChild {
		return ParentCounter{}, fmt.Errorf("%w: child counter already consumed %s", ErrConflict, expected)
	}
	// Consumption only retains the candidate. Next refuses further allocation
	// once the final uint64 value is stored; no increment happens here.
	value.LastChild = n
	return value, nil
}

// A missing parent counter is ErrNotFound. Consumers explicitly initialize it
// under its exact Update fact lock and the candidate's Update record lock.
// The prepared candidate may repair a stale counter from proven direct IDs;
// it is never reminted inside the transaction.
type ParentCounterReader interface {
	LoadParentCounter(context.Context, string) (ParentCounter, error)
}

type ParentCounterContinuation interface {
	ParentCounterReader
	ConsumeParentChild(context.Context, string, string) error
}

// AllocatorInventory is a complete typed snapshot census for loss prevention.
// Parent-only state never passes through OfflineAllocator or RestoreAllocators.
// A native archive without allocator support must refuse either nonempty list.
type AllocatorInventory struct {
	Projects []OfflineAllocator
	Parents  []ParentCounter
}

type AllocatorInventoryReader interface {
	AllocatorInventory(context.Context) (AllocatorInventory, error)
}

// AddRow decodes the internal persistence union without admitting an internal
// parent row as a project profile. Unknown namespaces/identities fail closed.
func (inventory *AllocatorInventory) AddRow(key string, data []byte, incarnation string) error {
	if strings.HasPrefix(key, parentCounterNamespace) {
		var value ParentCounter
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		expected, err := ParentCounterKey(incarnation, value.Parent)
		if err != nil {
			return err
		}
		if key != expected {
			return fmt.Errorf("parent counter row key differs from identity")
		}
		if err := value.Validate(incarnation, value.Parent); err != nil {
			return err
		}
		inventory.Parents = append(inventory.Parents, value)
		return nil
	}
	var value OfflineAllocator
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.Prefix != key {
		return fmt.Errorf("allocator row key differs from profile prefix")
	}
	if err := value.Validate(); err != nil {
		return err
	}
	inventory.Projects = append(inventory.Projects, value.Clone())
	return nil
}
