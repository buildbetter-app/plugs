package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// RestoreImportProfileAccounting admits named temporary/output candidates.
// JSON counts one compact wire payload; Bytes counts one byte payload. Neither
// callback includes a guessed concurrency multiplier. Releases are explicit,
// idempotent, and valid without a context, including refused partial work.
// The zero value preserves the existing unaccounted library API.
type RestoreImportProfileAccounting struct {
	Bytes func(phase string, size uint64) (func(), error)
	JSON  func(phase string, value any) (func(), error)
}

func (a RestoreImportProfileAccounting) reserveBytes(phase string, size uint64) (func(), error) {
	if a.Bytes == nil {
		return func() {}, nil
	}
	release, err := a.Bytes(phase, size)
	if release == nil {
		release = func() {}
	}
	return release, err
}
func (a RestoreImportProfileAccounting) reserveJSON(phase string, value any) (func(), error) {
	if a.JSON == nil {
		return func() {}, nil
	}
	release, err := a.JSON(phase, value)
	if release == nil {
		release = func() {}
	}
	return release, err
}
func profileReleaseAll(releases *[]func()) func() {
	released := false
	return func() {
		if released {
			return
		}
		released = true
		for i := len(*releases) - 1; i >= 0; i-- {
			(*releases)[i]()
		}
		*releases = nil
	}
}

// DetachedWithAccounting returns ownership of the detached scalar and raw-plan
// copies. The caller must release them when the returned profile ceases to be
// held; temporary validation/serializer candidates have already ended.
// NormalizeWithAccounting validates and normalizes scalar fields, borrowing the
// immutable canonical plan for synchronous use. The caller must not publish the
// borrowed profile or retain it beyond the input's lifetime.
func (p RestoreImportProfile) NormalizeWithAccounting(accounting RestoreImportProfileAccounting) (RestoreImportProfile, func(), error) {
	return p.detachedImportProfile(false, accounting)
}

func (p RestoreImportProfile) DetachedWithAccounting(accounting RestoreImportProfileAccounting) (RestoreImportProfile, func(), error) {
	return p.detachedImportProfile(true, accounting)
}

func (p RestoreImportProfile) detachedImportProfile(copyPlan bool, accounting RestoreImportProfileAccounting) (RestoreImportProfile, func(), error) {
	if p.Method != "offline.restore.import" || len(p.IdempotencyKey) < 16 || ValidateCheckpointKey(p.IdempotencyKey) != nil || ValidateCheckpointKey(string(p.SourceStoreID)) != nil || !importDigest(p.ArchiveDigest) || !importDigest(p.DerivedDigest) || !importDigest(p.Admission.AdmissionDigest) {
		return RestoreImportProfile{}, nil, fmt.Errorf("invalid prepared import binding")
	}
	if p.HistoryDigest != "" && !importDigest(p.HistoryDigest) {
		return RestoreImportProfile{}, nil, fmt.Errorf("invalid history payload digest")
	}
	if p.HistoryTime != "" {
		when, err := time.Parse(time.RFC3339Nano, p.HistoryTime)
		if err != nil || when.IsZero() || when.UTC().Format(time.RFC3339Nano) != p.HistoryTime {
			return RestoreImportProfile{}, nil, fmt.Errorf("invalid canonical history restore time")
		}
	}
	historyMode, err := ParseRestoreHistoryMode(string(p.HistoryMode))
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	p.HistoryMode = historyMode
	if p.NativeDeltaHistory != nil {
		if err := p.NativeDeltaHistory.Validate(historyMode); err != nil {
			return RestoreImportProfile{}, nil, err
		}
		if p.SourcePlanes == nil {
			return RestoreImportProfile{}, nil, fmt.Errorf("native delta history selection requires a joint source plan")
		}
	}
	binding := p.SourcePlanes
	if binding != nil {
		plan, done, e := binding.PlanWithAccounting(accounting)
		if e != nil {
			return RestoreImportProfile{}, nil, e
		}
		matches := plan.SourceStoreID == string(p.SourceStoreID) && plan.ArchiveSHA256 == p.ArchiveDigest
		done()
		if !matches {
			return RestoreImportProfile{}, nil, sourcePlaneError("joint plan differs from import source/archive")
		}
	}

	a := p.Admission
	if (a.Mode == "authenticated" && a.Scope != "") || (a.Mode == "anonymous-local" && a.Scope != "" && ValidateCheckpointKey(a.Scope) != nil) {
		return RestoreImportProfile{}, nil, fmt.Errorf("import admission scope is not the normalized G4 agent label")
	}
	if !((a.Mode == "authenticated" && a.Authority == "restore-admin" && a.Actor.Kind == "user") || (a.Mode == "anonymous-local" && a.Authority == "trusted-human-presence" && a.Actor.Kind == "anonymous")) {
		return RestoreImportProfile{}, nil, fmt.Errorf("import admission mode/actor/authority mismatch")
	}
	// Scalar normalization keeps the predecessor's custom principal codec,
	// UseNumber, invalid-UTF8 handling, and complete JSON field normalization.
	// The checked canonical plan is a separate byte domain, never base64 JSON
	// input to the scalar decoder.
	p.SourcePlanes = nil
	scalarOutput, err := accounting.reserveJSON("profile_scalar_output", p)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	output := []func(){scalarOutput}
	releaseOutput := profileReleaseAll(&output)
	success := false
	defer func() {
		if !success {
			releaseOutput()
		}
	}()
	wire, err := accounting.reserveJSON("profile_scalar_wire", p)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	defer wire()
	encode, err := accounting.reserveJSON("profile_scalar_encoder", p)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	data, err := json.Marshal(p)
	encode()
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	decode, err := accounting.reserveJSON("profile_scalar_decoder", p)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var detached RestoreImportProfile
	err = decoder.Decode(&detached)
	decode()
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	if binding != nil {
		value := *binding
		if copyPlan {
			done, err := accounting.reserveBytes("profile_canonical_output", uint64(len(binding.CanonicalPlan)))
			if err != nil {
				return RestoreImportProfile{}, nil, err
			}
			output = append(output, done)
			value.CanonicalPlan = bytes.Clone(binding.CanonicalPlan)
		}
		detached.SourcePlanes = &value
	}
	success = true
	return detached, releaseOutput, nil
}
