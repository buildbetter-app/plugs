package driver

import (
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// RestoreMode identifies the history contract a restore operation must keep.
// A current-image restore publishes only the admitted head image. A
// full-history restore must retain the source versions and changesets; it may
// never be treated as a current-image restore when the target lacks that
// capability.
type RestoreMode string

const (
	RestoreModeCurrentImage RestoreMode = "current-image"
	RestoreModeFullHistory  RestoreMode = "full-history"
)

// Recognized dump history declarations. The converter uses the provenance
// values as well as the native head/full selectors; keeping them here lets a
// restore adapter validate the declaration before it acquires a lease.
const (
	RestoreHistoryHead                = "head"
	RestoreHistoryFull                = "full"
	RestoreHistoryVerified            = "verified"
	RestoreHistoryUnavailable         = "unavailable"
	RestoreHistoryAvailableUnverified = "available-unverified"
)

const (
	// CapabilityRestoreCurrentImage is the capability used by the existing
	// atomic WriteTx-based restore path.
	CapabilityRestoreCurrentImage = "restore.current-image"
	// CapabilityRestoreFullHistory is deliberately separate from the ordinary
	// history read capability. A driver must implement an offline history port
	// before it may advertise this capability.
	CapabilityRestoreFullHistory = "restore.full-history"
)

// Supports reports whether a driver advertises one exact capability. It does
// not infer restore support from the ordinary "history" capability: reading
// history and injecting source history are different contracts.
func (i Info) Supports(capability string) bool {
	for _, advertised := range i.Capabilities {
		if advertised == capability {
			return true
		}
	}
	return false
}

func (m RestoreMode) valid() bool {
	switch m {
	case RestoreModeCurrentImage, RestoreModeFullHistory:
		return true
	default:
		return false
	}
}

// RestoreModeForHistory maps the plug-dump history discriminator to the
// required driver contract. The archive default is full history, so an
// omitted discriminator fails closed instead of silently flattening history.
func RestoreModeForHistory(history string) (RestoreMode, error) {
	switch history {
	case "", RestoreHistoryFull, RestoreHistoryVerified:
		return RestoreModeFullHistory, nil
	case RestoreHistoryHead, RestoreHistoryUnavailable:
		return RestoreModeCurrentImage, nil
	case RestoreHistoryAvailableUnverified:
		// Source rows marked available-unverified cannot be flattened into a
		// current image, even if a caller asks for head mode. The evidence is
		// not a verified history import and must refuse before any write.
		return "", ErrRestoreUnsupported{Mode: RestoreModeFullHistory, History: history}
	default:
		return "", ErrRestoreHistory{Value: history}
	}
}

// RestoreCapabilities is an optional store capability. Implementations must
// return true only when their restore path can preserve the complete mode
// named by the caller. In particular, a current-image writer must return
// false for RestoreModeFullHistory.
type RestoreCapabilities interface {
	SupportsRestore(RestoreMode) bool
}

// ErrRestoreHistory is a malformed or unknown history declaration. It is
// deliberately distinct from ErrRestoreUnsupported: malformed input must be
// rejected by validation, while a well-formed full-history input may be
// refused because the target only publishes a current image.
type ErrRestoreHistory struct {
	Value string
}

func (e ErrRestoreHistory) Error() string {
	return fmt.Sprintf("invalid restore history %q", e.Value)
}

func (e ErrRestoreHistory) driverMapping() (model.ErrorCode, map[string]any) {
	return "restore_violation", map[string]any{
		"reason":  "invalid_restore_history",
		"history": e.Value,
	}
}

// ErrRestoreUnsupported is a typed refusal for a restore mode that the target
// cannot preserve. It unwraps to ErrUnsupported so callers that only need the
// driver's closed capability sentinel continue to work, while restore
// adapters can retain the requested mode for a precise refusal.
type ErrRestoreUnsupported struct {
	Mode    RestoreMode
	History string
}

func (e ErrRestoreUnsupported) Error() string {
	if e.History != "" {
		return fmt.Sprintf("restore history %q requires unsupported %q capability", e.History, e.Mode)
	}
	if e.Mode == "" {
		return "restore capability is unsupported"
	}
	return fmt.Sprintf("restore mode %q is unsupported by the target driver", e.Mode)
}

func (e ErrRestoreUnsupported) Unwrap() error { return ErrUnsupported }

func (e ErrRestoreUnsupported) driverMapping() (model.ErrorCode, map[string]any) {
	details := map[string]any{
		"reason": "unsupported_restore_capability",
		"mode":   string(e.Mode),
	}
	if e.History != "" {
		details["history"] = e.History
	}
	return "restore_violation", details
}

// RequireRestoreCapability performs the read-only admission check that must
// happen after the archive's records, source history and digests have been
// validated but before a target lease, transaction or publication is opened.
// The value is intentionally accepted as any so callers can pass the
// engine-neutral driver.Store without changing the Store interface; only an
// optional RestoreCapabilities implementation can authorize the mode.
func RequireRestoreCapability(value any, mode RestoreMode) error {
	if !mode.valid() {
		return ErrRestoreUnsupported{Mode: mode}
	}
	capability, ok := value.(RestoreCapabilities)
	if !ok || !capability.SupportsRestore(mode) {
		return ErrRestoreUnsupported{Mode: mode}
	}
	return nil
}
