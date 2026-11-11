package driver

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestNativeDeltaHistoryPolicyBindsExactModeAndSourceFloor(t *testing.T) {
	profile := sourcePlaneTestProfile()
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	profile.HistoryMode = RestoreHistoryModePartial
	profile.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat, FirstSequence: 2}
	first, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	detached, err := profile.Detached()
	if err != nil {
		t.Fatal(err)
	}
	profile.NativeDeltaHistory.FirstSequence = 3
	second, err := profile.RequestHash()
	if err != nil || first == second || detached.NativeDeltaHistory.FirstSequence != 2 {
		t.Fatal("source floor was not detached/hash bound", err)
	}
	profile.HistoryMode = RestoreHistoryModeSnapshot
	profile.NativeDeltaHistory.FirstSequence = 0
	snapshot, err := profile.RequestHash()
	if err != nil || snapshot == first || snapshot == second {
		t.Fatal("snapshot policy not bound", err)
	}
	profile.HistoryMode = RestoreHistoryModeFull
	if _, err := profile.Detached(); err == nil {
		t.Fatal("Full accepted lossy source policy")
	}
	profile.NativeDeltaHistory = nil
	legacy, err := profile.Detached()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(legacy)
	if err != nil || bytes.Contains(raw, []byte("nativeDeltaHistory")) {
		t.Fatal("Full profile bytes changed", err)
	}
}

func TestNativeDeltaHistoryPolicyRejectsAmbiguousWire(t *testing.T) {
	for _, text := range []string{
		`null`,
		`{"format":"beads-native-delta-history-policy/1","format":"beads-native-delta-history-policy/1"}`,
		`{"format":"beads-native-delta-history-policy/1","firstSequence":2,"unknown":true}`,
		`{"format":"beads-native-delta-history-policy/1","firstSequence":-1}`,
		`{"format":"beads-native-delta-history-policy/1","firstSequence":18446744073709551616}`,
	} {
		var policy NativeDeltaHistoryPolicy
		if err := json.Unmarshal([]byte(text), &policy); err == nil {
			t.Fatalf("ambiguous policy accepted: %s", text)
		}
	}
	for _, tc := range []struct {
		mode  RestoreHistoryMode
		first uint64
	}{{RestoreHistoryModePartial, 0}, {RestoreHistoryModeSnapshot, 2}, {RestoreHistoryModeFull, 0}} {
		if err := (NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat, FirstSequence: tc.first}).Validate(tc.mode); err == nil {
			t.Fatal("invalid mode/floor accepted", tc)
		}
	}
}

func TestNativeDeltaSnapshotMetaRequiresValidPolicy(t *testing.T) {
	profile := sourcePlaneTestProfile()
	profile.HistoryMode = RestoreHistoryModeSnapshot
	profile.HistoryDigest = ""
	profile.HistoryTime = "2026-11-11T12:00:00Z"
	profile.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: "unknown"}
	meta := Meta{Head: model.Natural64(1), HistoryFloor: model.Natural64(1)}
	if err := ValidateSourcePlaneRestoreMeta(profile, meta); err == nil {
		t.Fatal("snapshot metadata accepted an unknown policy format")
	}
	profile.NativeDeltaHistory.Format = NativeDeltaHistoryPolicyFormat
	if err := ValidateSourcePlaneRestoreMeta(profile, meta); err != nil {
		t.Fatal("explicit snapshot metadata refused", err)
	}
	profile.NativeDeltaHistory.FirstSequence = 2
	if err := ValidateSourcePlaneRestoreMeta(profile, meta); err == nil {
		t.Fatal("snapshot metadata accepted a partial source floor")
	}
}

func TestNativeDeltaPartialHistoryKeepsBaselineSuffixAndUnknownPrefixSeparate(t *testing.T) {
	profile := sourcePlaneTestProfile()
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	profile.HistoryMode = RestoreHistoryModePartial
	profile.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat, FirstSequence: 2}
	evidence := NativeDeltaHistoryEvidence{
		Format: NativeDeltaHistoryEvidenceFormat, Mode: RestoreHistoryModePartial,
		ArchiveSHA256: profile.ArchiveDigest, SourceStoreID: string(profile.SourceStoreID), Project: "pl",
		FirstSequence: 2, FirstCommitID: "source-two", FirstRootSHA256: strings.Repeat("2", 64),
		HeadSequence: 3, HeadCommitID: "source-three", HeadRootSHA256: strings.Repeat("3", 64), CurrentRootSHA256: strings.Repeat("4", 64),
		BaselineSequence: 1, BaselineCommitID: "source-one", BaselineRootSHA256: strings.Repeat("1", 64),
		OmittedPrefix: &SourceHistoryGap{First: 1, Last: 1, Reason: "operator-selected-floor"}, UnavailablePrefixSHA256: strings.Repeat("d", 64),
	}
	before := Record{Ref: model.RefByID(NativeTableHistoryRecordPrefix+"partial-table", model.Natural64(0)), Kind: "beads-archive", Body: []byte(`{"value":9007199254740993}`)}
	after := before
	after.Body = []byte(`{"value":9007199254740995}`)
	history := OfflineHistory{
		Source: NativeTableHistorySource, Verified: true, First: 2, Head: 3,
		BaselineVerified: true, Baseline: []Record{before}, Gaps: []SourceHistoryGap{*evidence.OmittedPrefix}, NativeDeltaHistory: &evidence,
		NativeBoundaries: []NativeHistoryBoundary{{SourceStoreID: evidence.SourceStoreID, Project: "pl", VerifiedCommitCount: 2, BoundarySHA256: evidence.UnavailablePrefixSHA256}},
		Batches: []SourceHistoryBatch{
			{SourceStoreID: evidence.SourceStoreID, Project: "pl", SourceSequence: 2, Sequence: 2, CommitIDs: []string{"source-two"}, Changes: []SourceHistoryChange{{Before: &before, After: &after}}},
			{SourceStoreID: evidence.SourceStoreID, Project: "pl", SourceSequence: 3, Sequence: 3, CommitIDs: []string{"source-three"}, Changes: []SourceHistoryChange{{Before: &after}}},
		},
	}
	when := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	plan, err := BuildSourcePlaneHistory(Meta{}, profile, history, when, []Record{})
	if err != nil {
		t.Fatal("verified baseline/suffix refused", err)
	}
	if len(plan.Entries) != 4 || plan.Meta.HistoryFloor != model.Natural64(1) || len(plan.Evidence.Mapping) != 2 || plan.Evidence.Complete || len(plan.Evidence.Gaps) != 1 || len(plan.Evidence.NativeBoundaries) != 1 {
		t.Fatal("partial history fabricated prefix mappings or lost its separate boundary")
	}
	if len(plan.Versions) == 0 || !bytes.Equal(plan.Versions[0].Body, before.Body) || plan.Evidence.Mapping[0].SourceSequence != 2 || plan.Evidence.Mapping[0].Revision != model.Natural64(2) || plan.Evidence.Mapping[1].SourceSequence != 3 || plan.Evidence.Mapping[1].Revision != model.Natural64(3) {
		t.Fatal("verified predecessor became source history or suffix mapping shifted")
	}
	for _, fault := range []string{"missing-baseline", "changed-preimage", "missing-gap", "missing-boundary", "wrong-source", "wrong-commit", "wrong-floor"} {
		t.Run(fault, func(t *testing.T) {
			data, err := json.Marshal(history)
			if err != nil {
				t.Fatal(err)
			}
			var bad OfflineHistory
			if err := json.Unmarshal(data, &bad); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "missing-baseline":
				bad.Baseline = nil
			case "changed-preimage":
				bad.Baseline[0].Body = []byte(`{"value":9007199254740992}`)
			case "missing-gap":
				bad.Gaps = nil
			case "missing-boundary":
				bad.NativeBoundaries = nil
			case "wrong-source":
				bad.Batches[0].SourceStoreID = "unrelated"
			case "wrong-commit":
				bad.Batches[0].CommitIDs[0] = "unrelated"
			case "wrong-floor":
				bad.NativeDeltaHistory.FirstSequence = 3
			}
			if _, err := BuildSourcePlaneHistory(Meta{}, profile, bad, when, []Record{}); err == nil {
				t.Fatal("unproven partial history admitted")
			}
		})
	}
}
