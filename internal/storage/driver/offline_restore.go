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
	"time"
	"unicode"
	"unicode/utf8"

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
	StoreID          StoreID
	AllocatorBinding NativeAllocatorBinding
	Principal        model.PrincipalRef
	Method           string
	AgentLabel       string
	IdempotencyKey   string
	RequestHash      string
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
	if err := p.AllocatorBinding.Validate(); err != nil {
		return NativeRestoreProfile{}, err
	}
	if p.AllocatorBinding.StoreID != p.StoreID {
		return NativeRestoreProfile{}, NewNativeAllocatorSnapshotError(NativeAllocatorSourceMismatch, nil)
	}
	p.AllocatorBinding.Projects = append([]string{}, p.AllocatorBinding.Projects...)
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

// RestoreImportAdmission records a trusted host's prior admission. Structural
// validation is not authentication: no RPC accepts this binding object. The
// host must enforce the selected mode's authority and operation-bound presence.
type RestoreImportAdmission struct {
	Mode            string             `json:"mode"`
	Actor           model.PrincipalRef `json:"actor"`
	Scope           string             `json:"scope"`
	Authority       string             `json:"authority"`
	AdmissionDigest string             `json:"admissionDigest"`
}

// RestoreImportProfile is distinct from both the historical converter receipt
// and native admin.restore.finish. Its hash binds the complete admitted profile
// through the durable receipt; lookup by G4 identity alone is insufficient.
type RestoreImportProfile struct {
	Admission      RestoreImportAdmission `json:"admission"`
	SourceStoreID  StoreID                `json:"sourceStoreId"`
	Method         string                 `json:"method"`
	IdempotencyKey string                 `json:"idempotencyKey"`
	ArchiveDigest  string                 `json:"archiveDigest"`
	DerivedDigest  string                 `json:"derivedDigest"`
	// HistoryMode is explicit policy for archive history. Empty input is
	// normalized to full by Detached; snapshot and partial are opt-in.
	HistoryMode RestoreHistoryMode `json:"historyMode,omitempty"`
	// HistoryDigest binds the exact optional verified replay payload.
	HistoryDigest string `json:"historyDigest,omitempty"`
	// HistoryTime binds the actual restore time before historical imported
	// comment images are lowered and hashed. It is never a source timestamp.
	HistoryTime        string                    `json:"historyTime,omitempty"`
	NativeDeltaHistory *NativeDeltaHistoryPolicy `json:"nativeDeltaHistory,omitempty"`
	// SourcePlanes is the single checked joint logical plan binding. Nil keeps
	// the historical profile bytes and core-derived digest contract unchanged.
	SourcePlanes *SourcePlaneImportBindingV1 `json:"sourcePlanes,omitempty"`
}

type RestoreImportTx interface {
	PrepareRestoreImport(context.Context, RestoreImportProfile) (Meta, error)
}

func importDigest(value string) bool {
	return len(value) == 64 && strings.Trim(value, "0123456789abcdef") == ""
}

func (p RestoreImportProfile) Detached() (RestoreImportProfile, error) {
	detached, _, err := p.detachedImportProfile(true, RestoreImportProfileAccounting{})
	return detached, err
}

func (p RestoreImportProfile) RequestHash() (string, error) {
	return p.RequestHashWithAccounting(RestoreImportProfileAccounting{})
}

// RequestHashWithAccounting keeps the complete validated request hash while
// admitting each temporary wire candidate before it is allocated. Canonical
// plan bytes are borrowed only for this synchronous call.
func (p RestoreImportProfile) RequestHashWithAccounting(accounting RestoreImportProfileAccounting) (string, error) {
	detached, release, err := p.detachedImportProfile(false, accounting)
	if err != nil {
		return "", err
	}
	defer release()
	return hashRestoreImportProfile(detached, accounting)
}

// DetachedCoreOnly is the execution boundary for the historical importer.
// A checked joint plan is valid preparation input, but cannot be executed by
// a core-only writer that has no provider/origin/receipt correspondence.
func (p RestoreImportProfile) DetachedCoreOnly() (RestoreImportProfile, error) {
	bound, err := p.Detached()
	if err != nil {
		return RestoreImportProfile{}, err
	}
	if bound.SourcePlanes != nil {
		return RestoreImportProfile{}, sourcePlaneError("joint import requires atomic source-plane execution capability")
	}
	return bound, nil
}

// AgentLabel retains existing G4 normalization; full admission scope remains
// in RequestHash for both modes and must be compared again on recovery.
func (p RestoreImportProfile) AgentLabel() string {
	if p.Admission.Actor.Kind == "anonymous" {
		return p.Admission.Scope
	}
	return ""
}

func (p RestoreImportProfile) ValidateHeader(h ChangesetHeader) error {
	hash, err := p.RequestHash()
	if err != nil {
		return err
	}
	got, err := json.Marshal(h.Principal)
	if err != nil {
		return err
	}
	want, err := json.Marshal(p.Admission.Actor)
	if err != nil {
		return err
	}
	if h.Kind != model.ChangesetRestore || h.ResponsePending || h.Time.IsZero() || h.Method != p.Method || h.AgentLabel != p.AgentLabel() || h.IdempotencyKey != p.IdempotencyKey || h.RequestHash != hash || !bytes.Equal(got, want) {
		return fmt.Errorf("import header differs from its prepared profile")
	}
	for _, change := range h.Changes {
		if change.Op != model.OpCreate || !change.FromRevision.IsZero() {
			return fmt.Errorf("import requires only new record bases")
		}
	}
	return nil
}

// ValidateRecord binds imported comments to the admitted actor and the exact
// commit time. It is called only for the distinct prepared import capability.
func (p RestoreImportProfile) ValidateRecord(record Record, commitTime time.Time) error {
	if record.Kind != "bead" {
		return nil
	}
	var document struct {
		Comments []struct {
			Body         string             `json:"body"`
			Principal    model.PrincipalRef `json:"principal"`
			Time         time.Time          `json:"time"`
			Origin       string             `json:"origin"`
			SourceAuthor string             `json:"sourceAuthor"`
			SourceTime   *time.Time         `json:"sourceTime"`
		} `json:"comments"`
	}
	if err := json.Unmarshal(record.Body, &document); err != nil {
		return err
	}
	want, err := json.Marshal(p.Admission.Actor)
	if err != nil {
		return err
	}
	for _, comment := range document.Comments {
		actor, err := json.Marshal(comment.Principal)
		if err != nil {
			return err
		}
		if !bytes.Equal(actor, want) || comment.Origin != "import" || !comment.Time.Equal(commitTime) || comment.SourceTime == nil || comment.SourceTime.IsZero() {
			return fmt.Errorf("imported comment differs from admitted actor/time/provenance")
		}
		author := comment.SourceAuthor
		if !utf8.ValidString(author) || strings.TrimSpace(author) != author || utf8.RuneCountInString(author) > 256 {
			return fmt.Errorf("invalid imported source author")
		}
		for _, r := range author {
			if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
				return fmt.Errorf("invalid imported source author")
			}
		}
	}
	return nil
}
