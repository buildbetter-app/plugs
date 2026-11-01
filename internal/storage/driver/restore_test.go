package driver

import (
	"errors"
	"strings"
	"testing"
)

type restoreCapabilityFixture struct {
	currentImage bool
	fullHistory  bool
}

func (f restoreCapabilityFixture) SupportsRestore(mode RestoreMode) bool {
	switch mode {
	case RestoreModeCurrentImage:
		return f.currentImage
	case RestoreModeFullHistory:
		return f.fullHistory
	default:
		return false
	}
}

func TestRestoreModeForHistoryDefaultsToFullHistory(t *testing.T) {
	tests := []struct {
		history string
		want    RestoreMode
	}{
		{history: "", want: RestoreModeFullHistory},
		{history: "full", want: RestoreModeFullHistory},
		{history: "verified", want: RestoreModeFullHistory},
		{history: "head", want: RestoreModeCurrentImage},
		{history: "unavailable", want: RestoreModeCurrentImage},
	}
	for _, tt := range tests {
		got, err := RestoreModeForHistory(tt.history)
		if err != nil || got != tt.want {
			t.Fatalf("history %q -> mode %q, err %v; want %q", tt.history, got, err, tt.want)
		}
	}
	for _, history := range []string{"partial", "available-unverified"} {
		err := func() error {
			_, err := RestoreModeForHistory(history)
			return err
		}()
		if err == nil {
			t.Fatalf("unsupported history mode %q was accepted", history)
		}
		if history == "partial" {
			var malformed ErrRestoreHistory
			if !errors.As(err, &malformed) || malformed.Value != history {
				t.Fatalf("malformed history error=%T %v", err, err)
			}
		} else {
			var unsupported ErrRestoreUnsupported
			if !errors.As(err, &unsupported) || unsupported.History != history || unsupported.Mode != RestoreModeFullHistory {
				t.Fatalf("unverified history error=%T %v", err, err)
			}
		}
	}
}

func TestRestoreCapabilityDoesNotConflateReadableHistoryWithRestore(t *testing.T) {
	info := Info{Capabilities: []string{"history", CapabilityRestoreCurrentImage}}
	if !info.Supports("history") || !info.Supports(CapabilityRestoreCurrentImage) {
		t.Fatal("advertised capabilities were not reported")
	}
	if info.Supports(CapabilityRestoreFullHistory) {
		t.Fatal("current-image capability implied full-history restore")
	}
}

func TestRequireRestoreCapabilityRefusesFullHistoryBeforeMutation(t *testing.T) {
	fixture := restoreCapabilityFixture{currentImage: true}
	if err := RequireRestoreCapability(fixture, RestoreModeCurrentImage); err != nil {
		t.Fatalf("current-image capability refused: %v", err)
	}
	err := RequireRestoreCapability(fixture, RestoreModeFullHistory)
	var typed ErrRestoreUnsupported
	if err == nil || !errors.As(err, &typed) || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("full-history refusal lost its typed unsupported contract: %T %v", err, err)
	}
	if typed.Mode != RestoreModeFullHistory {
		t.Fatalf("refusal mode=%q", typed.Mode)
	}
	if !strings.Contains(err.Error(), string(RestoreModeFullHistory)) {
		t.Fatalf("refusal omitted mode: %v", err)
	}
	code, details, ok := ErrorMapping(err)
	if !ok || code != "restore_violation" || details["mode"] != string(RestoreModeFullHistory) {
		t.Fatalf("refusal mapping=%q %#v ok=%v", code, details, ok)
	}
}

func TestRequireRestoreCapabilityRejectsUnknownMode(t *testing.T) {
	err := RequireRestoreCapability(restoreCapabilityFixture{currentImage: true, fullHistory: true}, RestoreMode("unknown"))
	var typed ErrRestoreUnsupported
	if err == nil || !errors.As(err, &typed) || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unknown mode did not fail closed: %T %v", err, err)
	}
}

func TestRequireRestoreCapabilityRequiresOptionalProvider(t *testing.T) {
	err := RequireRestoreCapability(struct{}{}, RestoreModeCurrentImage)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("missing provider error=%v", err)
	}
}

func TestMalformedHistoryMapsToRestoreViolation(t *testing.T) {
	_, err := RestoreModeForHistory("partial")
	code, details, ok := ErrorMapping(err)
	if !ok || code != "restore_violation" || details["reason"] != "invalid_restore_history" || details["history"] != "partial" {
		t.Fatalf("malformed history mapping=%q %#v ok=%v", code, details, ok)
	}
}

func TestRestoreHistoryPolicyDefaultsToStrictFullAndKeepsTypedReasons(t *testing.T) {
	mode, err := ParseRestoreHistoryMode("")
	if err != nil || mode != RestoreHistoryModeFull {
		t.Fatalf("empty history mode=%q err=%v", mode, err)
	}
	if mode, err = ParseRestoreHistoryMode("head"); err != nil || mode != RestoreHistoryModeSnapshot {
		t.Fatalf("head compatibility mode=%q err=%v", mode, err)
	}
	for _, test := range []struct {
		name    string
		history string
		reason  RestoreHistoryReason
	}{
		{name: "unavailable", history: RestoreHistoryUnavailable, reason: RestoreHistoryReasonUnavailable},
		{name: "available-unverified", history: RestoreHistoryAvailableUnverified, reason: RestoreHistoryReasonAvailableUnverified},
		{name: "non-contiguous", history: string(RestoreHistoryReasonNonContiguous), reason: RestoreHistoryReasonNonContiguous},
		{name: "unmappable", history: string(RestoreHistoryReasonUnmappable), reason: RestoreHistoryReasonUnmappable},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRestoreHistoryPolicy(RestoreHistoryModeFull, test.history)
			var refused ErrRestoreHistoryRefused
			if !errors.As(err, &refused) || refused.Mode != RestoreHistoryModeFull || refused.Reason != test.reason {
				t.Fatalf("full history=%q err=%T %v", test.history, err, err)
			}
			code, details, ok := ErrorMapping(err)
			if !ok || code != "restore_violation" || details["reason"] != string(test.reason) {
				t.Fatalf("mapping=%q %#v ok=%v", code, details, ok)
			}
		})
	}
	err = ValidateRestoreHistoryPolicy(RestoreHistoryModePartial, RestoreHistoryUnavailable)
	var partialRefused ErrRestoreHistoryRefused
	if !errors.As(err, &partialRefused) || partialRefused.Mode != RestoreHistoryModePartial || partialRefused.Reason != RestoreHistoryReasonNotYetSupported {
		t.Fatalf("partial unavailable error=%T %v", err, err)
	}
	if err := ValidateRestoreHistoryPolicy(RestoreHistoryModeSnapshot, RestoreHistoryAvailableUnverified); err != nil {
		t.Fatalf("snapshot rejected source disclosure: %v", err)
	}
}
