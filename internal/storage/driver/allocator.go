package driver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// OfflineAllocator is the durable project allocator state carried by a
// restore. Prefix and mode are part of the identity: callers must not replace
// an admitted allocator with a different interpretation of the same prefix.
// Children contains direct-child counters only; grandchildren never advance a
// parent's counter.
type OfflineAllocator struct {
	Prefix           string            `json:"prefix"`
	Mode             string            `json:"id_mode"`
	HashLength       uint32            `json:"hash_length,omitempty"`
	MinHashLength    uint32            `json:"min_hash_length,omitempty"`
	MaxHashLength    uint32            `json:"max_hash_length,omitempty"`
	MaxCollisionProb float64           `json:"max_collision_prob,omitempty"`
	LastID           uint64            `json:"last_id"`
	Children         map[string]uint64 `json:"child_counters,omitempty"`
	RandomBytes      uint32            `json:"random_bytes,omitempty"`
	SuffixEncoding   string            `json:"suffix_encoding,omitempty"`
	TopLevelShape    string            `json:"top_level_shape,omitempty"`
	ChildShape       string            `json:"child_shape,omitempty"`
}

// HashInput is the stable content tuple used by the Beads hash allocator.
// The caller owns the clock and principal mapping; this type deliberately
// carries no ambient time or process state.
type HashInput struct {
	Title       string
	Description string
	Creator     string
	Timestamp   time.Time
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
// Historical hash declarations remain provenance only; live profiles include
// explicit adaptive settings and use the content-bound minting contract.
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
		max := a.MaxHashLength
		if max == 0 {
			max = a.HashLength
		}
		if max < 3 {
			return fmt.Errorf("hash allocator requires a hash length of at least 3")
		}
		if a.MinHashLength != 0 && (a.MinHashLength < 3 || a.MinHashLength > max) {
			return fmt.Errorf("hash allocator minimum length is outside the configured range")
		}
		if math.IsNaN(a.MaxCollisionProb) || math.IsInf(a.MaxCollisionProb, 0) || a.MaxCollisionProb < 0 || a.MaxCollisionProb > 1 {
			return fmt.Errorf("hash allocator collision probability must be between 0 and 1")
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

// NewHashAllocator returns a live hash allocator profile. The explicit
// adaptive fields distinguish a configured producer from historical source
// declarations that are retained only as provenance and must remain
// fail-closed until their policy is admitted.
func NewHashAllocator(prefix string, min, max uint32, probability float64) OfflineAllocator {
	if min == 0 {
		min = 3
	}
	if max == 0 {
		max = 8
	}
	if probability == 0 {
		probability = 0.25
	}
	return OfflineAllocator{Prefix: prefix, Mode: AllocatorHash, HashLength: max, MinHashLength: min, MaxHashLength: max, MaxCollisionProb: probability}
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

// NextChildID returns the next direct child ID and never reuses a number. Hash
// allocators use the same numeric hierarchical suffix as sequential ones.
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
	if a.Mode != AllocatorSequential && a.Mode != AllocatorHash {
		return "", fmt.Errorf("%w: unsupported child allocator continuation", ErrUnsupported)
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

// ChildNumber validates and decodes the canonical direct-child suffix. The
// allocator drivers use it when reconciling a stale restored counter against
// an observed child ID. Source decimal suffixes may contain leading zeroes,
// as Beads' ParseHierarchicalID accepts them; newly minted suffixes do not.
func ChildNumber(parent, expected string) (uint64, error) {
	if err := canonicalAllocatorPart("parent", parent); err != nil {
		return 0, err
	}
	prefix := parent + "."
	suffix, ok := strings.CutPrefix(expected, prefix)
	if !ok || suffix == "" || strings.ContainsRune(suffix, '.') {
		return 0, fmt.Errorf("invalid direct child candidate")
	}
	n, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil {
		// A decimal-only suffix that cannot fit in the durable counter is a
		// typed exhaustion case. Keep malformed/nondecimal candidates as
		// ordinary shape errors so callers can distinguish bad input from a
		// saturated restored counter.
		if strings.Trim(suffix, "0123456789") == "" {
			return 0, fmt.Errorf("%w: child counter overflow", ErrUnsupported)
		}
		return 0, fmt.Errorf("invalid direct child candidate")
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid direct child candidate")
	}
	return n, nil
}

// ValidateChildCandidate checks only the shape and allocator family of a
// direct child. Counter reconciliation is intentionally separate: a restored
// counter may lag an explicitly imported child, so the driver must accept the
// observed next suffix and repair the durable counter on commit.
func (a OfflineAllocator) ValidateChildCandidate(parent, expected string) (uint64, error) {
	if err := a.Validate(); err != nil {
		return 0, err
	}
	if a.Mode != AllocatorSequential && a.Mode != AllocatorHash && a.Mode != AllocatorRandom {
		return 0, fmt.Errorf("%w: unsupported child allocator continuation", ErrUnsupported)
	}
	if a.Mode == AllocatorRandom {
		return 0, fmt.Errorf("%w: random allocator uses an opaque child candidate", ErrUnsupported)
	}
	if !strings.HasPrefix(parent, a.Prefix+"-") {
		return 0, fmt.Errorf("parent is outside allocator prefix")
	}
	n, err := ChildNumber(parent, expected)
	if err != nil {
		return 0, err
	}
	if expected != fmt.Sprintf("%s.%d", parent, n) {
		return 0, fmt.Errorf("invalid direct child candidate")
	}
	return n, nil
}

// HashCandidate implements the pinned Beads content hash. Prefix is kept out
// of the digest. Requested lengths 3, 4, 5/6 and 7/8 use 2, 3, 4 and 5
// leading digest bytes respectively; all other lengths use the three-byte
// fallback while retaining exact output width.
func HashCandidate(prefix string, input HashInput, length, nonce uint32) (string, error) {
	if err := canonicalAllocatorPart("prefix", prefix); err != nil {
		return "", err
	}
	if length < 1 {
		return "", fmt.Errorf("hash length must be positive")
	}
	content := strings.Join([]string{input.Title, input.Description, input.Creator, strconv.FormatInt(input.Timestamp.UnixNano(), 10), strconv.FormatUint(uint64(nonce), 10)}, "|")
	digest := sha256.Sum256([]byte(content))
	bytes := 3
	switch length {
	case 3:
		bytes = 2
	case 4:
		bytes = 3
	case 5, 6:
		bytes = 4
	case 7, 8:
		bytes = 5
	}
	value := new(big.Int).SetBytes(digest[:bytes]).Text(36)
	if uint32(len(value)) < length {
		value = strings.Repeat("0", int(length)-len(value)) + value
	} else if uint32(len(value)) > length {
		value = value[len(value)-int(length):]
	}
	return prefix + "-" + value, nil
}

func (a OfflineAllocator) hashRange() (min, max uint32, probability float64, configured bool, err error) {
	if err = a.Validate(); err != nil {
		return 0, 0, 0, false, err
	}
	if a.Mode != AllocatorHash {
		return 0, 0, 0, false, fmt.Errorf("%w: not a hash allocator", ErrUnsupported)
	}
	max = a.MaxHashLength
	if max == 0 {
		max = a.HashLength
	}
	min = a.MinHashLength
	if min == 0 {
		min = 3
	}
	probability = a.MaxCollisionProb
	if probability == 0 && a.MinHashLength == 0 && a.MaxHashLength == 0 {
		probability = 0.25
	}
	// A plain hash_length declaration is source provenance, not enough
	// information to reconstruct the adaptive policy. Keep that historical
	// state valid for restore/readback but fail closed for live minting.
	configured = a.MinHashLength != 0 || a.MaxHashLength != 0 || a.MaxCollisionProb != 0
	return min, max, probability, configured, nil
}

func adaptiveHashLength(count uint64, min, max uint32, probability float64) uint32 {
	for length := min; length <= max; length++ {
		possibilities := math.Pow(36, float64(length))
		n := float64(count)
		collision := 1 - math.Exp(-(n*n)/(2*possibilities))
		if collision <= probability {
			return length
		}
	}
	return max
}

// MintHashID tries exactly ten nonces at each adaptive length and advances
// through the configured maximum. occupied is evaluated for every candidate
// against the caller's immutable snapshot plus any staged IDs.
func (a OfflineAllocator) MintHashID(input HashInput, count uint64, occupied func(string) (bool, error)) (string, error) {
	min, max, probability, configured, err := a.hashRange()
	if err != nil {
		return "", err
	}
	if !configured {
		return "", fmt.Errorf("%w: hash allocator policy is incomplete", ErrUnsupported)
	}
	start := adaptiveHashLength(count, min, max, probability)
	for length := start; length <= max; length++ {
		for nonce := uint32(0); nonce < 10; nonce++ {
			candidate, err := HashCandidate(a.Prefix, input, length, nonce)
			if err != nil {
				return "", err
			}
			used, err := occupied(candidate)
			if err != nil {
				return "", err
			}
			if !used {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf("%w: hash candidates exhausted", ErrUnsupported)
}

// ValidateHashCandidate binds a previewed candidate to the configured hash
// profile and input tuple. It intentionally accepts any configured length and
// nonce in the finite source contract; the caller's immutable count chooses
// the adaptive starting length during MintHashID.
func (a OfflineAllocator) ValidateHashCandidate(input HashInput, expected string) error {
	min, max, _, configured, err := a.hashRange()
	if err != nil {
		return err
	}
	if !configured {
		return fmt.Errorf("%w: hash allocator policy is incomplete", ErrUnsupported)
	}
	for length := min; length <= max; length++ {
		for nonce := uint32(0); nonce < 10; nonce++ {
			candidate, e := HashCandidate(a.Prefix, input, length, nonce)
			if e != nil {
				return e
			}
			if candidate == expected {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: hash candidate is outside configured nonce/length bounds", ErrConflict)
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
	if a.Mode == AllocatorHash && parent == "" {
		max := a.MaxHashLength
		if max == 0 {
			max = a.HashLength
		}
		prefix := a.Prefix + "-"
		suffix, ok := strings.CutPrefix(expected, prefix)
		if !ok || len(suffix) < 3 || uint32(len(suffix)) > max || strings.Trim(suffix, "0123456789abcdefghijklmnopqrstuvwxyz") != "" {
			return fmt.Errorf("invalid hash allocator candidate")
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

// HashAllocatorContinuation binds a previewed content candidate to the same
// allocator fact and candidate record lock used by ordinary allocation. The
// content tuple is checked at the transaction boundary by implementations
// that support the configured hash profile.
type HashAllocatorContinuation interface {
	AllocatorContinuation
	AllocateHashTopLevel(context.Context, string, HashInput, string) error
}

// MarshalJSON keeps historical encodings intact. Configured hash profiles
// always emit their probability, including an explicit zero; random profiles
// omit source counters.
func (a OfflineAllocator) MarshalJSON() ([]byte, error) {
	type plain OfflineAllocator
	if a.Mode == AllocatorHash && (a.MinHashLength != 0 || a.MaxHashLength != 0) {
		return json.Marshal(struct {
			plain
			Probability float64 `json:"max_collision_prob"`
		}{plain(a), a.MaxCollisionProb})
	}
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
