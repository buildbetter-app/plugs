package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// SourcePlaneCommitRecordID is immutable import evidence. It is published in
// the same changeset and original receipt as the initial provider namespaces.
const SourcePlaneCommitRecordID = "instance_config/restore-import/source-planes"

// SourcePlaneRestoreTx is separate from the historical core-only importer.
// Preparation binds the complete checked plan to the actual empty target.
// Commit requires every participant's staged checkpoint, evidence and receipt.
type SourcePlaneRestoreTx interface {
	PrepareSourcePlaneImport(context.Context, RestoreImportProfile) (Meta, error)
	BindSourcePlaneImport(context.Context, SourcePlaneCommitBindingV1) error
}

// SourcePlaneHistoryRestoreTx joins complete verified core history with the
// initial source namespaces in the same atomic publication and original receipt.
type SourcePlaneHistoryRestoreTx interface {
	SourcePlaneRestoreTx
	OfflineHistoryTx
	PrepareSourcePlaneHistory(context.Context, RestoreImportProfile, OfflineHistory, time.Time) (Meta, error)
}

// PrepareSourcePlaneImportProfile validates execution preconditions before
// any new-incarnation or checkpoint staging. Snapshot is an explicit policy;
// the full-history adapter has a separate preparation capability below.
func PrepareSourcePlaneImportProfile(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime) (RestoreImportProfile, error) {
	return prepareSourcePlaneProfile(profile, current, lifetime, false)
}

func PrepareSourcePlaneHistoryProfile(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime) (RestoreImportProfile, error) {
	return prepareSourcePlaneProfile(profile, current, lifetime, true)
}

func prepareSourcePlaneProfile(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime, history bool) (RestoreImportProfile, error) {
	p, _, err := prepareSourcePlaneProfileWithAccounting(profile, current, lifetime, history, RestoreImportProfileAccounting{})
	return p, err
}

// PrepareSourcePlaneHistoryProfileWithAccounting preserves joint history
// admission and transfers only the detached output; validation scratch is local.
func PrepareSourcePlaneHistoryProfileWithAccounting(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime, accounting RestoreImportProfileAccounting) (RestoreImportProfile, func(), error) {
	return prepareSourcePlaneProfileWithAccounting(profile, current, lifetime, true, accounting)
}
func prepareSourcePlaneProfileWithAccounting(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime, history bool, accounting RestoreImportProfileAccounting) (RestoreImportProfile, func(), error) {
	p, release, err := profile.DetachedWithAccounting(accounting)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	if p.SourcePlanes == nil {
		return RestoreImportProfile{}, nil, sourcePlaneError("joint import requires the complete source plan")
	}
	if history {
		if p.HistoryMode != RestoreHistoryModeFull && !(p.HistoryMode == RestoreHistoryModePartial && p.NativeDeltaHistory != nil) || p.HistoryDigest == "" || p.HistoryTime == "" {
			return RestoreImportProfile{}, nil, sourcePlaneError("joint history requires full verified replay binding")
		}
	} else if p.HistoryMode != RestoreHistoryModeSnapshot || p.HistoryDigest != "" || p.HistoryTime != "" && p.NativeDeltaHistory == nil || p.NativeDeltaHistory != nil && p.HistoryTime == "" {
		return RestoreImportProfile{}, nil, sourcePlaneError("joint snapshot requires explicit snapshot policy")
	}
	plan, releasePlan, err := p.SourcePlanes.PlanWithAccounting(accounting)
	if err != nil {
		return RestoreImportProfile{}, nil, err
	}
	defer releasePlan()
	if err := validateSourcePlaneRestoreProviders(plan); err != nil {
		return RestoreImportProfile{}, nil, err
	}
	if plan.Target.StoreID != string(current.StoreID) || plan.Target.Incarnation != current.Incarnation || plan.Target.Head != 0 || !current.Head.IsZero() || !current.HistoryFloor.IsZero() || !plan.Target.RequireEmpty || plan.Target.Lifetime != lifetime {
		return RestoreImportProfile{}, nil, ErrConflict
	}
	success = true
	return p, release, nil
}

// SourcePlaneImportPublication is transaction-local, detached preparation.
// Its final validation consumes the actual stages, not a provider assertion.
type SourcePlaneImportPublication struct {
	profile RestoreImportProfile
	meta    Meta
	binding *SourcePlaneCommitBindingV1
}

func NewSourcePlaneImportPublication(profile RestoreImportProfile, meta Meta) *SourcePlaneImportPublication {
	return &SourcePlaneImportPublication{profile: profile, meta: meta}
}

func (p *SourcePlaneImportPublication) Bind(binding SourcePlaneCommitBindingV1) error {
	if p == nil {
		return sourcePlaneError("joint import was not prepared")
	}
	if err := binding.Validate(p.profile, p.meta); err != nil {
		return err
	}
	if p.binding != nil && !reflect.DeepEqual(*p.binding, binding) {
		return ErrConflict
	}
	binding.Providers = append([]SourcePlaneProviderCommitV1(nil), binding.Providers...)
	p.binding = &binding
	return nil
}

func (b SourcePlaneCommitBindingV1) Validate(profile RestoreImportProfile, meta Meta) error {
	if profile.SourcePlanes == nil {
		return sourcePlaneError("missing joint profile")
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		return err
	}
	if err := validateSourcePlaneRestoreProviders(plan); err != nil {
		return err
	}
	request, err := profile.RequestHash()
	if err != nil {
		return err
	}
	if err := ValidateSourcePlaneRestoreMeta(profile, meta); err != nil {
		return err
	}
	if b.Version != SourcePlaneCommitVersionV1 || b.JointPlanDigest != profile.SourcePlanes.JointPlanDigest || b.RequestHash != request || b.StoreID != profile.SourceStoreID || b.StoreID != meta.StoreID || b.Incarnation != meta.Incarnation || b.Incarnation == plan.Target.Incarnation || b.Revision != meta.Head || len(b.Providers) != len(plan.Providers) {
		return sourcePlaneError("joint commit differs from prepared plan/target")
	}
	for i, entry := range b.Providers {
		want := plan.Providers[i]

		origin := SourcePlaneOriginBindingV1{Version: SourcePlaneOriginVersionV1, JointPlanDigest: b.JointPlanDigest, RequestHash: request, StoreID: meta.StoreID, Incarnation: meta.Incarnation, Namespace: want.Namespace, InitialGeneration: want.ExpectedGeneration + 1, LogicalImageSHA256: want.LogicalImageSHA256, Lifetime: want.Lifetime}
		if entry.Provider != want || entry.Origin != origin || !importDigest(entry.EnvelopeSHA256) {
			return sourcePlaneError("joint provider/origin differs from plan")
		}
	}
	return nil
}

// Validate joins namespace stages to both the staged immutable record and the
// original receipt image. A later StageCheckpoint or Put cannot evade it.
func (p *SourcePlaneImportPublication) Validate(header ChangesetHeader, checkpoints map[string]Checkpoint, records map[string]*Record) error {
	if p == nil || p.binding == nil {
		return sourcePlaneError("joint publication has no commit binding")
	}
	if err := p.profile.ValidateHeader(header); err != nil {
		return err
	}
	if err := p.binding.Validate(p.profile, p.meta); err != nil {
		return err
	}
	if len(checkpoints) != len(p.binding.Providers) || header.Response.Revision != p.binding.Revision {
		return sourcePlaneError("joint publication participant/receipt census differs")
	}
	for _, provider := range p.binding.Providers {
		cp, ok := checkpoints[provider.Provider.Namespace]
		if !ok {
			return sourcePlaneError("joint checkpoint stage differs from commit binding")
		}
		if err := ValidateSourcePlaneInitialCheckpoint(cp, provider); err != nil {
			return err
		}
	}
	data, err := json.Marshal(p.binding)
	if err != nil {
		return err
	}
	staged, receipted := 0, 0
	for _, record := range records {
		if record != nil && record.Ref.ID == SourcePlaneCommitRecordID {
			if record.Kind != "instance_config" || !record.Ref.Branch.IsZero() || !bytes.Equal(record.Body, data) {
				return sourcePlaneError("joint evidence stage differs")
			}
			staged++
		}
	}
	for _, result := range header.Response.Results {
		if result.Ref.ID == SourcePlaneCommitRecordID {
			if !result.Ref.Branch.IsZero() || result.Revision != p.binding.Revision || !bytes.Equal(result.Record, data) {
				return sourcePlaneError("joint evidence receipt differs")
			}
			receipted++
		}
	}
	if staged != 1 || receipted != 1 {
		return sourcePlaneError("joint evidence missing from stage/receipt")
	}
	return nil
}

// ValidateSourcePlaneRestoreMeta keeps the committed revision and floor bound
// to the selected history mode and its explicit native delta policy.
func ValidateSourcePlaneRestoreMeta(profile RestoreImportProfile, meta Meta) error {
	switch profile.HistoryMode {
	case RestoreHistoryModeSnapshot:
		clockBound := profile.HistoryTime == "" && profile.NativeDeltaHistory == nil
		if profile.NativeDeltaHistory != nil {
			clockBound = profile.HistoryTime != "" && profile.NativeDeltaHistory.Validate(profile.HistoryMode) == nil
		}
		if profile.HistoryDigest == "" && clockBound && meta.Head == model.Natural64(1) && meta.HistoryFloor == model.Natural64(1) {
			return nil
		}
	case RestoreHistoryModeFull:
		head, err := meta.Head.Uint64()
		if err == nil && head >= 2 && meta.HistoryFloor.IsZero() && profile.HistoryDigest != "" && profile.HistoryTime != "" && profile.NativeDeltaHistory == nil {
			return nil
		}
	case RestoreHistoryModePartial:
		if profile.NativeDeltaHistory != nil && profile.NativeDeltaHistory.Validate(profile.HistoryMode) == nil && profile.HistoryDigest != "" && profile.HistoryTime != "" {
			head, err := meta.Head.Uint64()
			floor := model.Natural64(0)
			if profile.NativeDeltaHistory.FirstSequence > 1 {
				floor = model.Natural64(1)
			}
			if err == nil && head >= 2 && meta.HistoryFloor == floor {
				return nil
			}
		}
	}
	return sourcePlaneError("joint restore revision/floor differs from explicit history policy")
}

// ValidateSourcePlaneInitialCheckpoint verifies the actual host envelope at
// initial publication. Recovery separately checks its immutable origin while
// allowing legitimate later logical images and namespace generations.
func ValidateSourcePlaneInitialCheckpoint(cp Checkpoint, provider SourcePlaneProviderCommitV1) error {
	sum := sha256.Sum256(cp.Payload)
	if cp.Generation != provider.Origin.InitialGeneration || hex.EncodeToString(sum[:]) != provider.EnvelopeSHA256 {
		return sourcePlaneError("joint checkpoint stage differs from commit binding")
	}
	origin, revision, value, err := ReadSourcePlaneCheckpointEnvelope(cp)
	if err != nil {
		return err
	}
	if origin != provider.Origin {
		return sourcePlaneError("joint checkpoint immutable origin differs")
	}
	sum = sha256.Sum256(value)
	if revision != 1 || uint64(len(value)) != provider.Provider.LogicalImageSize || hex.EncodeToString(sum[:]) != provider.Provider.LogicalImageSHA256 {
		return sourcePlaneError("joint checkpoint logical image differs")
	}
	logical, err := checkpointFields(value, 0)
	if err != nil {
		return err
	}
	for _, key := range []string{"sourcePlanes", "nativeDeltaHistory"} {
		if bytes.Equal(logical[key], []byte("null")) {
			return sourcePlaneError("explicit null sourcePlanes")
		}
	}
	return nil
}

func validateSourcePlaneRestoreProviders(plan SourcePlanePlanV1) error {
	if len(plan.Providers) != 1 {
		return sourcePlaneError("unsupported joint participant census")
	}
	want := plan.Providers[0]
	if want.Alias != "workflow" || want.Instance != "default" || want.Namespace != "plugin/workflow/default/durable" || want.Schema != "workflow/source-state/1" || want.Codec != "workflow/json/1" || want.OriginAdapter != "beads-workflow-origins/1" || want.ExpectedGeneration != 0 || want.Lifetime != plan.Target.Lifetime {
		return sourcePlaneError("unsupported joint provider adapter")
	}
	return nil
}
