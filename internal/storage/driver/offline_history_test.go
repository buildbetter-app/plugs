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
