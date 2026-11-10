package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func replayProfile(mode RestoreHistoryMode) RestoreImportProfile {
	return RestoreImportProfile{Admission: RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: "source", Method: "offline.restore.import", IdempotencyKey: "history-contract-fixture", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: mode}
}
func replayRecord(body string) *Record {
	return &Record{Ref: model.RefByID("pl-a", model.Natural64(0)), Kind: "bead", Body: []byte(body)}
}
func replayFixture() OfflineHistory {
	a, b := replayRecord(`{"title":"first","metadata":{"beads.agent_state":"opaque","null":null,"wide":9007199254740993}}`), replayRecord(`{"title":"second"}`)
	return OfflineHistory{Source: "beads-journal", Verified: true, Complete: true, First: 1, Head: 4, Batches: []SourceHistoryBatch{
		{Sequence: 1, EventIDs: []string{"journal:1"}, CommitIDs: []string{"dolt-a"}, Changes: []SourceHistoryChange{{After: a}}},
		{Sequence: 2, EventIDs: []string{"journal:2"}, Changes: []SourceHistoryChange{{Before: a, After: b}}},
		{Sequence: 3, EventIDs: []string{"journal:3"}, Changes: []SourceHistoryChange{{Before: b}}},
		{Sequence: 4, EventIDs: []string{"journal:4"}, Changes: []SourceHistoryChange{{After: a}}},
	}}
}
func TestOfflineHistoryBuildsOnlyVerifiedImagesWithImportAttribution(t *testing.T) {
	p := replayProfile(RestoreHistoryModeFull)
	h := replayFixture()
	when := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	plan, err := BuildOfflineHistory(Meta{Incarnation: "old"}, p, h, when, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Meta.Incarnation == "old" || plan.Meta.Head.String() != "5" || !plan.Meta.HistoryFloor.IsZero() || len(plan.Entries) != 5 || len(plan.Versions) != 5 {
		t.Fatalf("plan: %+v", plan)
	}
	for i, e := range plan.Entries {
		if e.Revision.String() != model.Natural64(uint64(i+1)).String() || e.Principal.ID != "restore-admin" || !e.Time.Equal(when) || e.Incarnation != plan.Meta.Incarnation || e.Kind != model.ChangesetRestore {
			t.Fatalf("fabricated attribution/order: %+v", e)
		}
	}
	if !plan.Versions[0].Base || plan.Versions[1].Base || !plan.Versions[2].Deleted || !plan.Versions[3].Base || !bytes.Contains(plan.Versions[0].Body, []byte("9007199254740993")) {
		t.Fatalf("versions: %+v", plan.Versions)
	}
	if len(plan.Evidence.Mapping) != 4 || plan.Evidence.Mapping[0].EventIDs[0] != "journal:1" || plan.Evidence.Mapping[0].CommitIDs[0] != "dolt-a" {
		t.Fatal("source identifiers lost")
	}
	h.Batches[0].Changes[0].After.Body[0] = 'x'
	h.Batches[0].EventIDs[0] = "forged"
	if !json.Valid(plan.Versions[0].Body) || plan.Evidence.Mapping[0].EventIDs[0] != "journal:1" {
		t.Fatal("caller mutated prepared history")
	}
}
func TestOfflineHistoryRejectsUnverifiedGapsForgedImagesAndTailMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*OfflineHistory)
		reason RestoreHistoryReason
	}{
		{"unverified", func(h *OfflineHistory) { h.Verified = false }, RestoreHistoryReasonAvailableUnverified},
		{"missing-prefix", func(h *OfflineHistory) { h.Complete = false }, RestoreHistoryReasonNonContiguous},
		{"interior-gap", func(h *OfflineHistory) { h.Batches[1].Sequence = 3 }, RestoreHistoryReasonNonContiguous},
		{"missing-tail", func(h *OfflineHistory) { h.Head = 5 }, RestoreHistoryReasonNonContiguous},
		{"forged-before", func(h *OfflineHistory) { h.Batches[1].Changes[0].Before = replayRecord(`{"title":"forged"}`) }, RestoreHistoryReasonUnmappable},
		{"missing-before", func(h *OfflineHistory) { h.Batches[1].Changes[0].Before = nil }, RestoreHistoryReasonUnmappable},
		{"duplicate-event", func(h *OfflineHistory) { h.Batches[1].EventIDs = []string{"journal:1"} }, RestoreHistoryReasonUnmappable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := replayFixture()
			tc.mutate(&h)
			_, err := BuildOfflineHistory(Meta{}, replayProfile(RestoreHistoryModeFull), h, time.Now(), nil)
			var refused ErrRestoreHistoryRefused
			if !errors.As(err, &refused) || refused.Reason != tc.reason {
				t.Fatalf("err=%T %v", err, err)
			}
		})
	}
	_, err := BuildOfflineHistory(Meta{}, replayProfile(RestoreHistoryModeFull), replayFixture(), time.Now(), []Record{*replayRecord(`{"title":"forged head"}`)})
	if err == nil {
		t.Fatal("head mismatch admitted")
	}
}
func TestOfflineHistoryPartialRequiresProvenBaselineExactGapAndSuffix(t *testing.T) {
	a, b := replayRecord(`{"title":"first"}`), replayRecord(`{"title":"second"}`)
	h := OfflineHistory{Source: "beads-journal", Verified: true, First: 9, Head: 9, BaselineVerified: true, Baseline: []Record{*a}, Gaps: []SourceHistoryGap{{First: 1, Last: 8, Reason: "unavailable"}}, Batches: []SourceHistoryBatch{{Sequence: 9, EventIDs: []string{"journal:9"}, Changes: []SourceHistoryChange{{Before: a, After: b}}}}}
	plan, err := BuildOfflineHistory(Meta{Incarnation: "old"}, replayProfile(RestoreHistoryModePartial), h, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Meta.HistoryFloor.String() != "1" || plan.Meta.Head.String() != "3" || plan.Evidence.Mapping[0].Revision.String() != "2" || len(plan.Evidence.Gaps) != 1 {
		t.Fatalf("partial=%+v", plan)
	}
	for _, mutate := range []func(*OfflineHistory){func(h *OfflineHistory) { h.BaselineVerified = false }, func(h *OfflineHistory) { h.Gaps = nil }, func(h *OfflineHistory) { h.Gaps[0].Last = 9 }} {
		bad := h
		bad.Gaps = append([]SourceHistoryGap(nil), h.Gaps...)
		mutate(&bad)
		if _, err := BuildOfflineHistory(Meta{}, replayProfile(RestoreHistoryModePartial), bad, time.Now(), nil); err == nil {
			t.Fatal("unproved partial accepted")
		}
	}
}

func TestOfflineHistoryRetainsExplicitCommitWithoutInventingAnEventOrChange(t *testing.T) {
	h := replayFixture()
	h.Head = 5
	h.Batches = append(h.Batches, SourceHistoryBatch{Sequence: 5, CommitIDs: []string{"source-empty-commit"}, EventIDs: []string{}, Changes: []SourceHistoryChange{}})
	when := time.Now().UTC()
	p := replayProfile(RestoreHistoryModeFull)
	p.HistoryTime = when.Format(time.RFC3339Nano)
	p.HistoryDigest, _ = OfflineHistoryDigest(h)
	plan, err := BuildOfflineHistory(Meta{}, p, h, when, nil)
	if err != nil || len(plan.Entries[4].Changes) != 0 || plan.Evidence.Mapping[4].CommitIDs[0] != "source-empty-commit" || len(plan.Evidence.Mapping[4].EventIDs) != 0 {
		t.Fatalf("empty source commit was invented or dropped: %+v %v", plan, err)
	}
	if _, err = PrepareOfflineHistoryBinding(Meta{}, p, h, when.Add(time.Second)); err == nil {
		t.Fatal("restore time drifted from historical comment binding")
	}
	h.Batches[4].CommitIDs = nil
	if _, err = BuildOfflineHistory(Meta{}, p, h, when, nil); err == nil {
		t.Fatal("provenance-free empty source unit admitted")
	}
}

func TestOfflineHistoryScopesOriginalSequencesEventsAndCommitsBySource(t *testing.T) {
	fixture := func() OfflineHistory {
		h := OfflineHistory{Source: "explicit-project-history", Verified: true, Complete: true, First: 1, Head: 2}
		for i, project := range []string{"pl", "qa"} {
			r := Record{Ref: model.RefByID("source/"+project, model.Natural64(0)), Kind: "config", Body: []byte(`{"value":"same"}`)}
			h.Batches = append(h.Batches, SourceHistoryBatch{SourceStoreID: "store-" + project, Project: project, SourceSequence: 1, Sequence: uint64(i + 1), EventIDs: []string{"1"}, CommitIDs: []string{"same-raw-commit"}, Changes: []SourceHistoryChange{{After: &r}}})
		}
		return h
	}
	p := replayProfile(RestoreHistoryModeFull)
	plan, err := BuildOfflineHistory(Meta{}, p, fixture(), time.Now(), nil)
	if err != nil || len(plan.Evidence.Mapping) != 2 || len(plan.Records) != 3 {
		t.Fatal("independent source IDs were deduplicated or refused", err)
	}
	for i, project := range []string{"pl", "qa"} {
		mapping := plan.Evidence.Mapping[i]
		if mapping.SourceStoreID != "store-"+project || mapping.Project != project || mapping.SourceSequence != 1 || mapping.Sequence != uint64(i+1) || mapping.EventIDs[0] != "1" || mapping.CommitIDs[0] != "same-raw-commit" {
			t.Fatal("original source identity/sequence was rewritten", mapping)
		}
	}
	for _, fault := range []string{"duplicate-event-within-source", "source-gap", "missing-store"} {
		t.Run(fault, func(t *testing.T) {
			h := fixture()
			switch fault {
			case "duplicate-event-within-source":
				h.Batches[1].SourceStoreID, h.Batches[1].Project, h.Batches[1].SourceSequence = "store-pl", "pl", 2
			case "source-gap":
				h.Batches[1].SourceSequence = 2
			case "missing-store":
				h.Batches[1].SourceStoreID = ""
			}
			if _, err := BuildOfflineHistory(Meta{}, p, h, time.Now(), nil); err == nil {
				t.Fatal("ambiguous or incomplete original source history admitted")
			}
		})
	}
}

func TestNativeTableHistoryKeepsCurrentBusinessCreatesOutsideSourceCommits(t *testing.T) {
	when := time.Date(2026, 11, 9, 20, 0, 0, 0, time.UTC)
	profile := sourcePlaneTestProfile()
	profile.HistoryMode = RestoreHistoryModeFull
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	before := Record{Ref: model.RefByID(NativeTableHistoryRecordPrefix+"scoped-table", model.Natural64(0)), Kind: "beads-archive", Body: []byte(`{"columns":["id"],"rows":[{"id":1}]}`)}
	after := before
	after.Body = []byte(`{"columns":["id","status"],"rows":[{"id":1,"status":"open"}]}`)
	history := OfflineHistory{Source: NativeTableHistorySource, Verified: true, Complete: true, First: 1, Head: 2, Batches: []SourceHistoryBatch{
		{SourceStoreID: "original", Project: "pl", SourceSequence: 1, Sequence: 1, CommitIDs: []string{"old-schema"}, EventIDs: []string{}, Changes: []SourceHistoryChange{{After: &before}}},
		{SourceStoreID: "original", Project: "pl", SourceSequence: 2, Sequence: 2, CommitIDs: []string{"new-schema"}, EventIDs: []string{}, Changes: []SourceHistoryChange{{Before: &before, After: &after}}},
	}}
	current := *replayRecord(`{"title":"current model only"}`)
	tail := []Record{after, current}
	plan, err := BuildSourcePlaneHistory(Meta{}, profile, history, when, tail)
	if err != nil || len(plan.Entries) != 3 || len(plan.Evidence.Mapping) != 2 {
		t.Fatal("separate native history and current image refused", err)
	}
	for _, entry := range plan.Entries[:2] {
		for _, change := range entry.Changes {
			if change.Ref == current.Ref {
				t.Fatal("current portable bead assigned to a source commit")
			}
		}
	}
	if len(plan.Entries[2].Changes) != 2 || plan.Entries[2].Changes[0].Ref != current.Ref || plan.Evidence.Source != NativeTableHistorySource {
		t.Fatal("current creation or distinct raw history evidence lost")
	}
	for _, fault := range []string{"core-only", "portable-source", "portable-past-image", "unscoped", "fabricated-event", "changed-raw-head"} {
		t.Run(fault, func(t *testing.T) {
			bad := history
			bad.Batches = append([]SourceHistoryBatch(nil), history.Batches...)
			badTail := append([]Record(nil), tail...)
			joint := true
			p := profile
			switch fault {
			case "core-only":
				joint, p.SourcePlanes = false, nil
			case "portable-source":
				bad.Source = "beads-dolt-images/1"
			case "portable-past-image":
				bad.Batches[0].Changes = []SourceHistoryChange{{After: &current}}
			case "unscoped":
				bad.Batches[0].SourceStoreID = ""
			case "fabricated-event":
				bad.Batches[0].EventIDs = []string{"inferred-from-current-audit"}
			case "changed-raw-head":
				badTail[0] = before
			}
			var err error
			if joint {
				_, err = BuildSourcePlaneHistory(Meta{}, p, bad, when, badTail)
			} else {
				_, err = BuildOfflineHistory(Meta{}, p, bad, when, badTail)
			}
			if err == nil {
				t.Fatal("native evidence boundary was bypassed")
			}
		})
	}
}

func TestNativeTableHistoryBoundaryDoesNotFabricatePrefixOrCompleteness(t *testing.T) {
	when := time.Date(2026, 11, 10, 7, 0, 0, 0, time.UTC)
	profile := sourcePlaneTestProfile()
	profile.HistoryMode = RestoreHistoryModeFull
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	before := Record{Ref: model.RefByID(NativeTableHistoryRecordPrefix+"boundary-table", model.Natural64(0)), Kind: "beads-archive", Body: []byte(`{"columns":["id"],"rows":[{"id":1}]}`)}
	after := before
	after.Body = []byte(`{"columns":["id"],"rows":[{"id":2}]}`)
	history := OfflineHistory{Source: NativeTableHistorySource, Verified: true, Complete: false, First: 1, Head: 2,
		NativeBoundaries: []NativeHistoryBoundary{{SourceStoreID: "original", Project: "pl", VerifiedCommitCount: 2, BoundarySHA256: strings.Repeat("d", 64)}},
		Batches: []SourceHistoryBatch{
			{SourceStoreID: "original", Project: "pl", SourceSequence: 1, Sequence: 1, CommitIDs: []string{"oldest-verified"}, EventIDs: []string{}, Changes: []SourceHistoryChange{{After: &before}}},
			{SourceStoreID: "original", Project: "pl", SourceSequence: 2, Sequence: 2, CommitIDs: []string{"verified-head"}, EventIDs: []string{}, Changes: []SourceHistoryChange{{Before: &before, After: &after}}},
		},
	}
	plan, err := BuildSourcePlaneHistory(Meta{}, profile, history, when, []Record{after})
	if err != nil {
		t.Fatal("verified native images ending at a typed source boundary refused", err)
	}
	if plan.Evidence.Complete || len(plan.Evidence.Gaps) != 0 || plan.Evidence.First != 1 || plan.Evidence.Head != 2 || !plan.Evidence.Floor.IsZero() || len(plan.Evidence.Mapping) != 2 || len(plan.Entries) != 3 || plan.Evidence.NativeBoundaries[0] != history.NativeBoundaries[0] {
		t.Fatal("source boundary was lost, relabeled complete or padded with fabricated prefix revisions")
	}
	for i, mapping := range plan.Evidence.Mapping {
		if mapping.SourceSequence != uint64(i+1) || mapping.Revision != model.Natural64(uint64(i+1)) || len(mapping.EventIDs) != 0 || len(mapping.CommitIDs) != 1 {
			t.Fatal("unknown prefix or current events were invented as source history")
		}
	}
	// Two source namespaces retain independent boundary commitments and exact
	// verified-image counts. Neither source's unknown prefix becomes a global
	// gap or a baseline for the other source.
	joined := history
	joined.Head = 4
	joined.Batches = append([]SourceHistoryBatch(nil), history.Batches...)
	other := before
	other.Ref.ID += "-other-source"
	joined.Batches = append(joined.Batches,
		SourceHistoryBatch{SourceStoreID: "second", Project: "sv2", SourceSequence: 1, Sequence: 3, CommitIDs: []string{"second-oldest"}, EventIDs: []string{}, Changes: []SourceHistoryChange{{After: &other}}},
		SourceHistoryBatch{SourceStoreID: "second", Project: "sv2", SourceSequence: 2, Sequence: 4, CommitIDs: []string{"second-head"}, EventIDs: []string{}, Changes: []SourceHistoryChange{}},
	)
	joined.NativeBoundaries = append(append([]NativeHistoryBoundary(nil), history.NativeBoundaries...), NativeHistoryBoundary{SourceStoreID: "second", Project: "sv2", VerifiedCommitCount: 2, BoundarySHA256: strings.Repeat("e", 64)})
	joinedPlan, err := BuildSourcePlaneHistory(Meta{}, profile, joined, when, []Record{after, other})
	if err != nil || joinedPlan.Evidence.Complete || len(joinedPlan.Evidence.NativeBoundaries) != 2 || len(joinedPlan.Evidence.Mapping) != 4 || len(joinedPlan.Entries) != 5 {
		t.Fatal("independent verified-to-boundary source histories were combined or padded", err)
	}
	for i, boundary := range joined.NativeBoundaries {
		if joinedPlan.Evidence.NativeBoundaries[i] != boundary {
			t.Fatal("a source boundary changed namespace or descriptor commitment")
		}
	}
	for _, fault := range []string{"no-boundary", "falsely-complete", "known-gap", "fabricated-baseline", "baseline-claim", "wrong-count", "wrong-source", "duplicate-boundary", "bad-digest", "uppercase-digest", "missing-present-image", "contradictory-present-image", "portable-source"} {
		t.Run(fault, func(t *testing.T) {
			bad := history
			bad.NativeBoundaries = append([]NativeHistoryBoundary(nil), history.NativeBoundaries...)
			bad.Batches = append([]SourceHistoryBatch(nil), history.Batches...)
			switch fault {
			case "no-boundary":
				bad.NativeBoundaries = nil
			case "falsely-complete":
				bad.Complete = true
			case "known-gap":
				bad.Gaps = []SourceHistoryGap{{First: 1, Last: 1, Reason: "present image missing"}}
			case "fabricated-baseline":
				bad.Baseline = []Record{before}
			case "baseline-claim":
				bad.BaselineVerified = true
			case "wrong-count":
				bad.NativeBoundaries[0].VerifiedCommitCount++
			case "wrong-source":
				bad.NativeBoundaries[0].Project = "other"
			case "duplicate-boundary":
				bad.NativeBoundaries = append(bad.NativeBoundaries, bad.NativeBoundaries[0])
			case "bad-digest":
				bad.NativeBoundaries[0].BoundarySHA256 = strings.Repeat("z", 64)
			case "uppercase-digest":
				bad.NativeBoundaries[0].BoundarySHA256 = strings.Repeat("D", 64)
			case "missing-present-image":
				bad.Batches[1].SourceSequence++
			case "contradictory-present-image":
				bad.Batches[1].Changes = []SourceHistoryChange{{Before: &after, After: &before}}
			case "portable-source":
				bad.Source = "beads-dolt-images/1"
			}
			if _, err := BuildSourcePlaneHistory(Meta{}, profile, bad, when, []Record{after}); err == nil {
				t.Fatal("source boundary bypassed a present-history integrity check")
			}
		})
	}
}
