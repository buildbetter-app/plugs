package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const NativeAllocatorVersion = "plug-native-allocator/1"

type NativeAllocatorReason string

const (
	NativeAllocatorSnapshotMissing     NativeAllocatorReason = "native_allocator_snapshot_missing"
	NativeAllocatorProfileUnsupported  NativeAllocatorReason = "native_allocator_profile_unsupported"
	NativeAllocatorCoverageUnavailable NativeAllocatorReason = "native_allocator_coverage_unavailable"
	NativeAllocatorInventoryInvalid    NativeAllocatorReason = "native_allocator_inventory_invalid"
	NativeAllocatorSourceMismatch      NativeAllocatorReason = "native_allocator_source_mismatch"
	NativeAllocatorDigestMismatch      NativeAllocatorReason = "native_allocator_digest_mismatch"
	NativeAllocatorBindingRequired     NativeAllocatorReason = "native_allocator_binding_required"
)

func (r NativeAllocatorReason) Validate() error {
	switch r {
	case NativeAllocatorSnapshotMissing, NativeAllocatorProfileUnsupported, NativeAllocatorCoverageUnavailable, NativeAllocatorInventoryInvalid, NativeAllocatorSourceMismatch, NativeAllocatorDigestMismatch, NativeAllocatorBindingRequired:
		return nil
	default:
		return fmt.Errorf("invalid native allocator refusal reason")
	}
}

// NativeAllocatorSnapshotError is internal. Core maps its closed reason to
// the existing restore_violation catalog entry, never to a new public code.
type NativeAllocatorSnapshotError struct {
	Reason NativeAllocatorReason
	Cause  error
}

func NewNativeAllocatorSnapshotError(reason NativeAllocatorReason, cause error) error {
	if err := reason.Validate(); err != nil {
		return &NativeAllocatorSnapshotError{Reason: NativeAllocatorInventoryInvalid, Cause: err}
	}
	return &NativeAllocatorSnapshotError{Reason: reason, Cause: cause}
}

func (e *NativeAllocatorSnapshotError) Error() string {
	if e.Reason.Validate() != nil {
		return "invalid native allocator refusal reason"
	}
	if e.Cause != nil {
		return string(e.Reason) + ": " + e.Cause.Error()
	}
	return string(e.Reason)
}
func (e *NativeAllocatorSnapshotError) Unwrap() error        { return e.Cause }
func (e *NativeAllocatorSnapshotError) Is(target error) bool { return target == ErrUnsupported }

type AllocatorSnapshot struct {
	Version       string
	StoreID       StoreID
	Incarnation   string
	At            model.Revision
	CoverageFloor model.Revision
	Inventory     AllocatorInventory
}

type AllocatorSnapshotReader interface {
	AllocatorSnapshot(context.Context) (AllocatorSnapshot, error)
}
type NativeAllocatorRestoreTx interface {
	RestoreNativeAllocators(context.Context, AllocatorSnapshot) error
}

func NativeAllocatorRestoreLock() (LockKey, error) {
	fact, err := NewReadFact("native-allocator/restore")
	return LockKey{Kind: LockReadFact, Fact: fact}, err
}

// NewAllocatorSnapshot owns and sorts its complete input. The wire validator
// itself never repairs noncanonical arrays or infers a missing inventory.
func NewAllocatorSnapshot(meta Meta, at, floor model.Revision, inventory AllocatorInventory) (AllocatorSnapshot, error) {
	s := AllocatorSnapshot{Version: NativeAllocatorVersion, StoreID: meta.StoreID, Incarnation: meta.Incarnation, At: at, CoverageFloor: floor}
	s.Inventory.Projects = make([]OfflineAllocator, len(inventory.Projects))
	for i, a := range inventory.Projects {
		s.Inventory.Projects[i] = a.Clone()
	}
	s.Inventory.Parents = append([]ParentCounter{}, inventory.Parents...)
	sort.Slice(s.Inventory.Projects, func(i, j int) bool { return s.Inventory.Projects[i].Prefix < s.Inventory.Projects[j].Prefix })
	sort.Slice(s.Inventory.Parents, func(i, j int) bool { return s.Inventory.Parents[i].Parent < s.Inventory.Parents[j].Parent })
	return s, s.Validate()
}

func (s AllocatorSnapshot) Validate() error {
	invalid := func(err error) error { return NewNativeAllocatorSnapshotError(NativeAllocatorInventoryInvalid, err) }
	if s.Version != NativeAllocatorVersion {
		return NewNativeAllocatorSnapshotError(NativeAllocatorProfileUnsupported, nil)
	}
	if s.StoreID == "" || strings.TrimSpace(string(s.StoreID)) != string(s.StoreID) || strings.ContainsRune(string(s.StoreID), '\x00') {
		return invalid(fmt.Errorf("invalid source store identity"))
	}
	if err := canonicalAllocatorPart("incarnation", s.Incarnation); err != nil {
		return invalid(err)
	}
	at, err := s.At.Uint64()
	if err != nil {
		return invalid(err)
	}
	floor, err := s.CoverageFloor.Uint64()
	if err != nil {
		return invalid(err)
	}
	if floor > at {
		return NewNativeAllocatorSnapshotError(NativeAllocatorCoverageUnavailable, nil)
	}
	if s.Inventory.Projects == nil || s.Inventory.Parents == nil {
		return invalid(fmt.Errorf("both complete inventories are required"))
	}
	for i, a := range s.Inventory.Projects {
		if err := a.Validate(); err != nil {
			return invalid(err)
		}
		if i > 0 && s.Inventory.Projects[i-1].Prefix >= a.Prefix {
			return invalid(fmt.Errorf("projects are not strictly sorted"))
		}
	}
	for i, p := range s.Inventory.Parents {
		if err := p.Validate(s.Incarnation, p.Parent); err != nil {
			return invalid(err)
		}
		if i > 0 && s.Inventory.Parents[i-1].Parent >= p.Parent {
			return invalid(fmt.Errorf("parents are not strictly sorted"))
		}
	}
	return nil
}

func (s AllocatorSnapshot) Detached() (AllocatorSnapshot, error) {
	if err := s.Validate(); err != nil {
		return AllocatorSnapshot{}, err
	}
	return NewAllocatorSnapshot(Meta{StoreID: s.StoreID, Incarnation: s.Incarnation}, s.At, s.CoverageFloor, s.Inventory)
}

type allocatorSnapshotWire struct {
	Version       string             `json:"version"`
	StoreID       StoreID            `json:"store_id"`
	Incarnation   string             `json:"incarnation"`
	At            model.Revision     `json:"at"`
	CoverageFloor model.Revision     `json:"coverage_floor"`
	Projects      []OfflineAllocator `json:"projects"`
	Parents       []ParentCounter    `json:"parents"`
}

func EncodeAllocatorSnapshot(s AllocatorSnapshot) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(allocatorSnapshotWire{s.Version, s.StoreID, s.Incarnation, s.At, s.CoverageFloor, s.Inventory.Projects, s.Inventory.Parents})
}

func DecodeAllocatorSnapshot(raw []byte) (AllocatorSnapshot, error) {
	var w allocatorSnapshotWire
	if err := strictAllocatorJSON(raw, &w); err != nil {
		return AllocatorSnapshot{}, NewNativeAllocatorSnapshotError(NativeAllocatorInventoryInvalid, err)
	}
	s := AllocatorSnapshot{w.Version, w.StoreID, w.Incarnation, w.At, w.CoverageFloor, AllocatorInventory{Projects: w.Projects, Parents: w.Parents}}
	encoded, err := EncodeAllocatorSnapshot(s)
	if err != nil {
		return AllocatorSnapshot{}, err
	}
	if !bytes.Equal(raw, encoded) {
		return AllocatorSnapshot{}, NewNativeAllocatorSnapshotError(NativeAllocatorInventoryInvalid, fmt.Errorf("noncanonical allocator snapshot bytes"))
	}
	return s, nil
}

func (s AllocatorSnapshot) Digest() (string, error) {
	raw, err := EncodeAllocatorSnapshot(s)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}

type NativeAllocatorBinding struct {
	Version     string
	StoreID     StoreID
	Incarnation string
	At          model.Revision
	Digest      string
	Projects    []string
}

func (s AllocatorSnapshot) Binding() (NativeAllocatorBinding, error) {
	digest, err := s.Digest()
	prefixes := make([]string, len(s.Inventory.Projects))
	for i, a := range s.Inventory.Projects {
		prefixes[i] = a.Prefix
	}
	return NativeAllocatorBinding{Version: s.Version, StoreID: s.StoreID, Incarnation: s.Incarnation, At: s.At, Digest: digest, Projects: prefixes}, err
}
func (b NativeAllocatorBinding) Validate() error {
	if b.Version != NativeAllocatorVersion || b.StoreID == "" || canonicalAllocatorPart("incarnation", b.Incarnation) != nil || !importDigest(b.Digest) {
		return NewNativeAllocatorSnapshotError(NativeAllocatorBindingRequired, nil)
	}
	if _, err := b.At.Uint64(); err != nil {
		return NewNativeAllocatorSnapshotError(NativeAllocatorBindingRequired, err)
	}
	if b.Projects == nil {
		return NewNativeAllocatorSnapshotError(NativeAllocatorBindingRequired, nil)
	}
	for i, prefix := range b.Projects {
		if canonicalAllocatorPart("prefix", prefix) != nil || i > 0 && b.Projects[i-1] >= prefix {
			return NewNativeAllocatorSnapshotError(NativeAllocatorBindingRequired, nil)
		}
	}
	return nil
}
func (b NativeAllocatorBinding) Match(s AllocatorSnapshot) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if b.Version != s.Version || b.StoreID != s.StoreID || b.Incarnation != s.Incarnation || b.At.String() != s.At.String() {
		return NewNativeAllocatorSnapshotError(NativeAllocatorSourceMismatch, nil)
	}
	digest, err := s.Digest()
	if err != nil {
		return err
	}
	if b.Digest != digest {
		return NewNativeAllocatorSnapshotError(NativeAllocatorDigestMismatch, nil)
	}
	if len(b.Projects) != len(s.Inventory.Projects) {
		return NewNativeAllocatorSnapshotError(NativeAllocatorSourceMismatch, nil)
	}
	for i, prefix := range b.Projects {
		if prefix != s.Inventory.Projects[i].Prefix {
			return NewNativeAllocatorSnapshotError(NativeAllocatorSourceMismatch, nil)
		}
	}
	return nil
}

// Reject duplicate keys at every nesting level before decoding typed exact
// integers. DisallowUnknownFields also applies to nested project/parent rows.
func strictAllocatorJSON(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func() error
	value = func() error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return fmt.Errorf("duplicate or invalid JSON key")
				}
				seen[name] = true
				if err := value(); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := value(); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing allocator JSON")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	return d.Decode(target)
}
