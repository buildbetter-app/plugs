package driver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

var ErrRestoreTargetNotEmpty = errors.New("restore target is not empty")

// OfflineRestoreTx is an optional, trusted offline capability, never an RPC
// authorization bypass. Call only after complete source validation, Lock and
// Head, and before staging anything. Head is the global writer fence. Prepare
// checks all durable data, not just live records, and stages a new incarnation
// and a head-only history floor of 1. Commit repeats emptiness under the atomic
// publication fence, and publishes metadata with the records/log/receipt. A
// rollback changes nothing. Ordinary transactions never enter this mode.
type OfflineRestoreTx interface {
	PrepareOfflineRestore(context.Context, StoreID) (Meta, error)
}

func NewOfflineRestoreMeta(current Meta, source StoreID) (Meta, error) {
	if source == "" || strings.TrimSpace(string(source)) != string(source) || strings.ContainsRune(string(source), '\x00') {
		return Meta{}, fmt.Errorf("invalid restore source store identity")
	}
	var identity [32]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return Meta{}, err
	}
	current.StoreID = source
	current.Incarnation = hex.EncodeToString(identity[:])
	current.Head = model.Natural64(1)
	current.HistoryFloor = model.Natural64(1)
	return current, nil
}

func ValidateOfflineRestoreHeader(h ChangesetHeader) error {
	if h.Kind != model.ChangesetRestore || h.Method != "offline.restore" || h.Principal.Kind != "system" || h.Principal.ID != "plug-converter" || h.Principal.Sponsor != nil || len(h.Principal.Caveats) != 0 || h.ResponsePending || h.IdempotencyKey == "" || len(h.RequestHash) != 64 {
		return fmt.Errorf("offline restore requires a final keyed restore receipt")
	}
	_, err := hex.DecodeString(h.RequestHash)
	if err != nil {
		return err
	}
	for _, change := range h.Changes {
		if change.Op != model.OpCreate || !change.FromRevision.IsZero() {
			return fmt.Errorf("offline restore requires only new record bases")
		}
	}
	return nil
}

// NativeRestoreProfile binds one fully validated native archive to its real
// finish caller. StoreID is the exact bound Manifest.NativeStoreID, equal to
// Manifest.StoreID. RequestHash covers the complete uploaded archive bytes.
// This is transaction-local state; Commit still publishes the durable receipt.
type NativeRestoreProfile struct {
	StoreID        StoreID
	Principal      model.PrincipalRef
	Method         string
	AgentLabel     string
	IdempotencyKey string
	RequestHash    string
}

// NativeRestoreTx uses the same empty-target fence and atomic metadata
// publication as OfflineRestoreTx, with a separately prepared native receipt
// profile. Call after validation, the complete Lock set and Head, before any
// staging. Initialized identity-preserving augmentation is unsupported.
type NativeRestoreTx interface {
	PrepareNativeRestore(context.Context, NativeRestoreProfile) (Meta, error)
}

// Detached validates and copies mutable attribution before preparation binds
// it. It never repairs identity, normalizes keys, or selects a caller profile
// from a commit header. The source identity is also checked by metadata staging.
func (p NativeRestoreProfile) Detached() (NativeRestoreProfile, error) {
	if p.Method != "admin.restore.finish" || p.IdempotencyKey == "" || strings.TrimSpace(p.IdempotencyKey) != p.IdempotencyKey || strings.ContainsRune(p.IdempotencyKey, '\x00') || len(p.RequestHash) != 64 {
		return NativeRestoreProfile{}, fmt.Errorf("native restore requires a final keyed admin.restore.finish receipt")
	}
	if _, err := hex.DecodeString(p.RequestHash); err != nil {
		return NativeRestoreProfile{}, err
	}
	principal, err := PrincipalKey(p.Principal)
	if err != nil {
		return NativeRestoreProfile{}, err
	}
	if strings.HasPrefix(principal, "anonymous:") && p.AgentLabel != "" && (strings.TrimSpace(p.AgentLabel) != p.AgentLabel || strings.ContainsRune(p.AgentLabel, '\x00')) {
		return NativeRestoreProfile{}, fmt.Errorf("native restore requires a canonical anonymous agent label")
	}
	data, err := json.Marshal(p.Principal)
	if err != nil {
		return NativeRestoreProfile{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var detached model.PrincipalRef
	if err := decoder.Decode(&detached); err != nil {
		return NativeRestoreProfile{}, err
	}
	p.Principal = detached
	return p, nil
}

func (p NativeRestoreProfile) ValidateHeader(h ChangesetHeader) error {
	got, err := json.Marshal(h.Principal)
	if err != nil {
		return err
	}
	want, err := json.Marshal(p.Principal)
	if err != nil {
		return err
	}
	if h.Kind != model.ChangesetRestore || h.ResponsePending || h.Method != p.Method || h.AgentLabel != p.AgentLabel || h.IdempotencyKey != p.IdempotencyKey || h.RequestHash != p.RequestHash || !bytes.Equal(got, want) {
		return fmt.Errorf("native restore header differs from its prepared profile")
	}
	for _, change := range h.Changes {
		if change.Op != model.OpCreate || !change.FromRevision.IsZero() {
			return fmt.Errorf("native restore requires only new record bases")
		}
	}
	return nil
}
