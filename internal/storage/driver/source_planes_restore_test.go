package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func sourcePlanePublicationFixture(t *testing.T) (*SourcePlaneImportPublication, SourcePlaneCommitBindingV1, ChangesetHeader, map[string]Checkpoint, map[string]*Record) {
	t.Helper()
	plan := sourcePlaneTestPlan()
	profile := sourcePlaneTestProfile()
	profile.HistoryMode = RestoreHistoryModeSnapshot
	binding, err := NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	old := Meta{StoreID: StoreID(plan.Target.StoreID), Incarnation: plan.Target.Incarnation}
	profile, err = PrepareSourcePlaneImportProfile(profile, old, CheckpointLifetimeProcess)
	if err != nil {
		t.Fatal(err)
	}
	meta := Meta{StoreID: profile.SourceStoreID, Incarnation: "new-incarnation", Head: model.Natural64(1), HistoryFloor: model.Natural64(1)}
	request, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	origin := SourcePlaneOriginBindingV1{Version: SourcePlaneOriginVersionV1, JointPlanDigest: binding.JointPlanDigest, RequestHash: request, StoreID: meta.StoreID, Incarnation: meta.Incarnation, Namespace: plan.Providers[0].Namespace, InitialGeneration: 1, LogicalImageSHA256: plan.Providers[0].LogicalImageSHA256, Lifetime: CheckpointLifetimeProcess}
	envelope, err := json.Marshal(map[string]any{"values": map[string]any{"workflow/state": map[string]any{"revision": 1, "value": json.RawMessage(`{}`)}}, "sourceOrigin": origin})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(envelope)
	commit := SourcePlaneCommitBindingV1{Version: SourcePlaneCommitVersionV1, JointPlanDigest: binding.JointPlanDigest, RequestHash: request, StoreID: meta.StoreID, Incarnation: meta.Incarnation, Revision: model.Natural64(1), Providers: []SourcePlaneProviderCommitV1{{Provider: plan.Providers[0], Origin: origin, EnvelopeSHA256: hex.EncodeToString(sum[:])}}}
	body, err := json.Marshal(commit)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID(SourcePlaneCommitRecordID, model.Natural64(0))
	header := ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), Principal: profile.Admission.Actor, Method: profile.Method, IdempotencyKey: profile.IdempotencyKey, RequestHash: request, Response: model.Commit{Revision: model.Natural64(1), Results: []model.OpResult{{Ref: ref, Revision: model.Natural64(1), Record: body}}}}
	return NewSourcePlaneImportPublication(profile, meta), commit, header, map[string]Checkpoint{origin.Namespace: {Generation: 1, Payload: envelope}}, map[string]*Record{ref.ID: {Ref: ref, Kind: "instance_config", Body: body}}
}

func TestSourcePlaneFinalPublicationRequiresCompleteOriginalBinding(t *testing.T) {
	for _, fault := range []string{"missing-binding", "missing-checkpoint", "extra-checkpoint", "changed-generation", "changed-envelope", "missing-evidence", "changed-evidence", "missing-receipt", "changed-receipt", "valid"} {
		t.Run(fault, func(t *testing.T) {
			prepared, binding, header, checkpoints, records := sourcePlanePublicationFixture(t)
			if fault != "missing-binding" {
				if err := prepared.Bind(binding); err != nil {
					t.Fatal(err)
				}
			}
			key := binding.Providers[0].Provider.Namespace
			switch fault {
			case "missing-checkpoint":
				delete(checkpoints, key)
			case "extra-checkpoint":
				checkpoints["unexpected/namespace"] = Checkpoint{Generation: 1, Payload: []byte(`{}`)}
			case "changed-generation":
				cp := checkpoints[key]
				cp.Generation++
				checkpoints[key] = cp
			case "changed-envelope":
				cp := checkpoints[key]
				cp.Payload = append(bytes.Clone(cp.Payload), ' ')
				checkpoints[key] = cp
			case "missing-evidence":
				delete(records, SourcePlaneCommitRecordID)
			case "changed-evidence":
				records[SourcePlaneCommitRecordID].Body = []byte(`{}`)
			case "missing-receipt":
				header.Response.Results = nil
			case "changed-receipt":
				header.Response.Results[0].Revision = model.Natural64(2)
			}
			err := prepared.Validate(header, checkpoints, records)
			if fault == "valid" {
				if err != nil {
					t.Fatal("complete publication refused", err)
				}
			} else if !errors.Is(err, ErrUnsupported) {
				t.Fatal("incomplete publication accepted or refused at wrong boundary", err)
			}
		})
	}
}
