package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// OfflineAllocator is the durable project allocator state carried by a
// restore. Prefix and mode are part of the identity: callers must not replace
// an admitted allocator with a different interpretation of the same prefix.
// Children contains direct-child counters only; grandchildren never advance a
// parent's counter.
type OfflineAllocator struct {
	Prefix         string            `json:"prefix"`
	Mode           string            `json:"id_mode"`
	HashLength     uint32            `json:"hash_length,omitempty"`
	LastID         uint64            `json:"last_id"`
	Children       map[string]uint64 `json:"child_counters,omitempty"`
	RandomBytes    uint32            `json:"random_bytes,omitempty"`
	SuffixEncoding string            `json:"suffix_encoding,omitempty"`
	TopLevelShape  string            `json:"top_level_shape,omitempty"`
	ChildShape     string            `json:"child_shape,omitempty"`
}

// AllocatorState is the engine-neutral name used by callers that are not
// restoring an offline archive.
type AllocatorState = OfflineAllocator

const (
	AllocatorSequential = "sequential"
	AllocatorHash       = "hash"
	AllocatorRandom     = "random"
)

func canonicalAllocatorPart(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "/\x00") {
		return fmt.Errorf("allocator %s is not canonical", name)
	}
	return nil
}

// Validate checks the portable state before a driver is allowed to stage it.
// Hash state is valid evidence and is persisted, but this port deliberately
// refuses to mint from it until the hash collision/nonce contract is admitted.
func (a OfflineAllocator) Validate() error {
	if err := canonicalAllocatorPart("prefix", a.Prefix); err != nil {
		return err
	}
	switch a.Mode {
	case AllocatorRandom:
		if a.HashLength != 0 || a.LastID != 0 || len(a.Children) != 0 || a.RandomBytes != 16 || a.SuffixEncoding != "lower-hex" || a.TopLevelShape != "prefix-random" || a.ChildShape != "parent-random" {
			return fmt.Errorf("random allocator requires the fixed 16-byte lower-hex profile without counters")
		}
	case AllocatorSequential:
		if a.HashLength != 0 {
			return fmt.Errorf("sequential allocator cannot set hash length")
		}
	case AllocatorHash:
		if a.HashLength < 3 {
			return fmt.Errorf("hash allocator requires a hash length of at least 3")
		}
	default:
		return fmt.Errorf("%w: allocator mode %q", ErrUnsupported, a.Mode)
	}
	if a.Mode != AllocatorRandom && (a.RandomBytes != 0 || a.SuffixEncoding != "" || a.TopLevelShape != "" || a.ChildShape != "") {
		return fmt.Errorf("nonrandom allocator cannot set random profile fields")
	}
	for parent := range a.Children {
		if err := canonicalAllocatorPart("parent", parent); err != nil {
			return err
		}
	}
	return nil
}

func (a OfflineAllocator) Clone() OfflineAllocator {
	copy := a
	if a.Children != nil {
		copy.Children = make(map[string]uint64, len(a.Children))
		for parent, counter := range a.Children {
			copy.Children[parent] = counter
		}
	}
	return copy
}

// NextID returns the next sequential top-level ID. Hash allocators are
// intentionally fail-closed because a counter cannot prove hash continuity.
func (a OfflineAllocator) NextID() (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	if a.Mode == AllocatorRandom {
		return randomAllocatorID(a.Prefix + "-")
	}
	if a.Mode != AllocatorSequential {
		return "", fmt.Errorf("%w: hash allocator continuation", ErrUnsupported)
	}
	if a.LastID == math.MaxUint64 {
		return "", fmt.Errorf("%w: allocator counter exhausted", ErrUnsupported)
	}
	return fmt.Sprintf("%s-%d", a.Prefix, a.LastID+1), nil
}

// NextChildID returns the next direct child ID and never reuses a number.
func (a OfflineAllocator) NextChildID(parent string) (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	if a.Mode == AllocatorRandom {
		if err := canonicalAllocatorPart("parent", parent); err != nil {
			return "", err
		}
		if !strings.HasPrefix(parent, a.Prefix+"-") {
			return "", fmt.Errorf("parent is outside allocator prefix")
		}
		return randomAllocatorID(parent + ".")
	}
	if a.Mode != AllocatorSequential {
		return "", fmt.Errorf("%w: hash child allocator continuation", ErrUnsupported)
	}
	if err := canonicalAllocatorPart("parent", parent); err != nil {
		return "", err
	}
	current := a.Children[parent]
	if current == math.MaxUint64 {
		return "", fmt.Errorf("%w: child counter exhausted", ErrUnsupported)
	}
	return fmt.Sprintf("%s.%d", parent, current+1), nil
}

// NewRandomAllocator is live target state, never source hash provenance.
func NewRandomAllocator(prefix string) OfflineAllocator {
	return OfflineAllocator{Prefix: prefix, Mode: AllocatorRandom, RandomBytes: 16, SuffixEncoding: "lower-hex", TopLevelShape: "prefix-random", ChildShape: "parent-random"}
}

func randomAllocatorID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(bytes[:]), nil
}

// ValidateCandidate consumes the previewed candidate without drawing entropy.
// Membership, complete locks and transaction-local reservations remain the
// driver's responsibility.
func (a OfflineAllocator) ValidateCandidate(parent, expected string) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.Mode == AllocatorRandom {
		prefix := a.Prefix + "-"
		if parent != "" {
			if err := canonicalAllocatorPart("parent", parent); err != nil {
				return err
			}
			if !strings.HasPrefix(parent, prefix) {
				return fmt.Errorf("parent is outside allocator prefix")
			}
			prefix = parent + "."
		}
		suffix, ok := strings.CutPrefix(expected, prefix)
		if !ok || len(suffix) != 32 || strings.Trim(suffix, "0123456789abcdef") != "" {
			return fmt.Errorf("invalid random allocator candidate")
		}
		return nil
	}
	var next string
	var err error
	if parent == "" {
		next, err = a.NextID()
	} else {
		next, err = a.NextChildID(parent)
	}
	if err != nil {
		return err
	}
	if next != expected {
		return fmt.Errorf("%w: allocator expected %s, got %s", ErrConflict, next, expected)
	}
	return nil
}

// AllocatorFact is the optimistic read fact used to serialize all changes to
// one prefix. It is deliberately separate from the closed sentinel families.
func AllocatorFact(prefix string) (ReadFact, error) {
	if err := canonicalAllocatorPart("prefix", prefix); err != nil {
		return "", err
	}
	return NewReadFact("allocator/" + prefix)
}

// AllocatorLock returns the complete-lock entry callers must include before
// RestoreAllocators or an allocation advance. The existing Writer lock set
// rules remain in force; this helper never acquires a lock by itself.
func AllocatorLock(prefix string) (LockKey, error) {
	fact, err := AllocatorFact(prefix)
	if err != nil {
		return LockKey{}, err
	}
	return LockKey{Kind: LockReadFact, Fact: fact}, nil
}

func ValidateAllocatorSet(values []OfflineAllocator) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return err
		}
		if _, exists := seen[value.Prefix]; exists {
			return fmt.Errorf("%w: duplicate allocator prefix %q", ErrConflict, value.Prefix)
		}
		seen[value.Prefix] = struct{}{}
	}
	return nil
}

// AllocatorReader is an optional read capability. A missing prefix returns
// ErrNotFound. Implementations return detached state owned by the caller.
type AllocatorReader interface {
	LoadAllocator(context.Context, string) (OfflineAllocator, error)
}

// AllocatorContinuation is an optional WriteTx capability. Restore and mint
// operations must be staged after the complete allocator fact lock set is
// acquired, then committed through the ordinary WriteTx Commit call.
type AllocatorContinuation interface {
	AllocatorReader
	RestoreAllocators(context.Context, []OfflineAllocator) error
	AllocateTopLevel(context.Context, string, string) error
	AllocateChild(context.Context, string, string) error
}

// MarshalJSON keeps historical source/sequential encodings intact while live
// random state uses exactly the target profile, without source counters.
func (a OfflineAllocator) MarshalJSON() ([]byte, error) {
	type plain OfflineAllocator
	if a.Mode != AllocatorRandom {
		return json.Marshal(plain(a))
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Prefix         string `json:"prefix"`
		Mode           string `json:"id_mode"`
		RandomBytes    uint32 `json:"random_bytes"`
		SuffixEncoding string `json:"suffix_encoding"`
		TopLevelShape  string `json:"top_level_shape"`
		ChildShape     string `json:"child_shape"`
	}{a.Prefix, a.Mode, a.RandomBytes, a.SuffixEncoding, a.TopLevelShape, a.ChildShape})
}
