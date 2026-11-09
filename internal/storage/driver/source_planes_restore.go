package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

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

// PrepareSourcePlaneImportProfile validates execution preconditions before
// any new-incarnation or checkpoint staging. Source history remains inert;
// the existing full/partial core-only replay adapters retain their refusals.
func PrepareSourcePlaneImportProfile(profile RestoreImportProfile, current Meta, lifetime CheckpointLifetime) (RestoreImportProfile, error) {
	p, err := profile.Detached()
	if err != nil {
		return RestoreImportProfile{}, err
	}
	if p.SourcePlanes == nil || p.HistoryMode != RestoreHistoryModeSnapshot || p.HistoryDigest != "" || p.HistoryTime != "" {
		return RestoreImportProfile{}, sourcePlaneError("joint import requires the snapshot adapter")
	}
	plan, err := p.SourcePlanes.Plan()
	if err != nil {
		return RestoreImportProfile{}, err
	}
	if err := validateSourcePlaneRestoreProviders(plan); err != nil {
		return RestoreImportProfile{}, err
	}
	if plan.Target.StoreID != string(current.StoreID) || plan.Target.Incarnation != current.Incarnation || plan.Target.Head != 0 || !current.Head.IsZero() || !current.HistoryFloor.IsZero() || !plan.Target.RequireEmpty || plan.Target.Lifetime != lifetime {
		return RestoreImportProfile{}, ErrConflict
	}
	return p, nil
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
	if b.Version != SourcePlaneCommitVersionV1 || b.JointPlanDigest != profile.SourcePlanes.JointPlanDigest || b.RequestHash != request || b.StoreID != profile.SourceStoreID || b.StoreID != meta.StoreID || b.Incarnation != meta.Incarnation || b.Incarnation == plan.Target.Incarnation || b.Revision.String() != "1" || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" || len(b.Providers) != len(plan.Providers) {
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
			if !result.Ref.Branch.IsZero() || result.Revision != model.Natural64(1) || !bytes.Equal(result.Record, data) {
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

// ValidateSourcePlaneInitialCheckpoint verifies the actual host envelope at
// initial publication. Recovery separately checks its immutable origin while
// allowing legitimate later logical images and namespace generations.
func ValidateSourcePlaneInitialCheckpoint(cp Checkpoint, provider SourcePlaneProviderCommitV1) error {
	sum := sha256.Sum256(cp.Payload)
	if cp.Generation != provider.Origin.InitialGeneration || hex.EncodeToString(sum[:]) != provider.EnvelopeSHA256 {
		return sourcePlaneError("joint checkpoint stage differs from commit binding")
	}
	fields := map[string]bool{"values": true, "sourceOrigin": true}
	if err := sourcePlaneJSONObject(cp.Payload, fields, true); err != nil {
		return err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(cp.Payload, &envelope); err != nil {
		return err
	}
	if err := sourcePlaneJSONObject(envelope["sourceOrigin"], map[string]bool{"version": true, "jointPlanDigest": true, "requestHash": true, "storeId": true, "incarnation": true, "namespace": true, "initialGeneration": true, "logicalImageSHA256": true, "lifetime": true}, true); err != nil {
		return err
	}
	var origin SourcePlaneOriginBindingV1
	if err := json.Unmarshal(envelope["sourceOrigin"], &origin); err != nil || origin != provider.Origin {
		return sourcePlaneError("joint checkpoint immutable origin differs")
	}
	if err := sourcePlaneJSONObject(envelope["values"], map[string]bool{"workflow/state": true}, true); err != nil {
		return err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(envelope["values"], &values); err != nil {
		return err
	}
	value := values["workflow/state"]
	if err := sourcePlaneJSONObject(value, map[string]bool{"revision": true, "value": true}, true); err != nil {
		return err
	}
	var item struct {
		Revision uint64          `json:"revision"`
		Value    json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(value, &item); err != nil {
		return err
	}
	sum = sha256.Sum256(item.Value)
	if item.Revision != 1 || uint64(len(item.Value)) != provider.Provider.LogicalImageSize || hex.EncodeToString(sum[:]) != provider.Provider.LogicalImageSHA256 {
		return sourcePlaneError("joint checkpoint logical image differs")
	}
	return sourcePlaneJSONObject(item.Value, nil, false)
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
