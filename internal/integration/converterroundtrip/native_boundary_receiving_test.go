package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Declared synthetic adapter evidence only. The genuine shallow-source fixture
// must be supplied independently by the producer's original public run.
func syntheticNativeBoundaryReceiving(t *testing.T, project, category string) []byte {
	t.Helper()
	return syntheticNativeBoundaryFromArchive(t, nativeTableArchiveFixture(t, project, category))
}

func syntheticNativeBoundaryFromArchive(t *testing.T, input []byte) []byte {
	t.Helper()
	members := tarMembers(t, input)
	var native dumpformat.NativeHistory
	var portable dumpformat.SourceHistory
	decode(t, members[dumpformat.NativeHistoryMember], &native)
	decode(t, members[dumpformat.SourceHistoryMember], &portable)
	// The seven retained fixture commits use a..g. The unavailable parent
	// must be external to that census; an internal ID must keep refusing.
	frontier := strings.Repeat("v", 32)
	for _, commit := range native.Commits {
		if commit.ID == frontier {
			t.Fatal("synthetic unavailable frontier aliases a retained commit")
		}
	}
	native.Commits[0].Parents = []string{frontier}
	native.Complete = false
	portable.Commits = native.Commits
	portable.Range.Reason = "native-history-boundary"
	members[dumpformat.SourceHistoryMember] = encode(t, portable)
	native.HistoryCoverage = &dumpformat.NativeHistoryCoverage{
		Format: dumpformat.NativeHistoryCoverageFormat, Status: "verified-to-boundary", VerifiedCommitCount: uint64(len(native.Images)),
		Boundaries:       []dumpformat.NativeHistoryBoundary{{CommitID: frontier, ChildCommitIDs: []string{native.Commits[0].ID}, CommitOrdinalFromHead: uint64(len(native.Images) + 1), Reason: "ghost_parent"}},
		EventsAttachment: dumpformat.NativeEventsAttachment{Member: dumpformat.SourceHistoryMember, SHA256: hashBytes(members[dumpformat.SourceHistoryMember]), Rows: uint64(len(portable.Events))},
	}
	members[dumpformat.NativeHistoryMember] = encode(t, native)
	return nativeReceivingFixtureMembers(t, members)
}

// Compare the report transported by the executable against the authenticated
// source, including exact descriptor fields that a counts-only receipt loses.
func assertNativeReceivingHistoryReport(t *testing.T, archive *dumpformat.Archive, report Report, complete bool) {
	t.Helper()
	observedComplete := true
	var histories []NativeHistoryReport
	for _, source := range archive.NativeSourceHistories() {
		observedComplete = observedComplete && source.Complete
		histories = append(histories, NativeHistoryReport{
			Source:              dumpformat.SourceIdentity{StoreID: source.StoreID, Project: source.Projects[0]},
			Complete:            source.Complete,
			VerifiedCommitCount: uint64(len(source.Images)),
			Coverage:            source.HistoryCoverage,
		})
	}
	if complete {
		histories = nil
	}
	if observedComplete != complete || !reflect.DeepEqual(report.NativeHistories, histories) {
		t.Fatal("receiving report lost per-source completeness, verified count or exact boundary/events descriptor")
	}
}

// Recount from original source images, not from a lowerer's reported total.
// The first imported snapshot is a verified source image, never an assertion
// about a record's creation or an invented revision before the boundary.
func assertNativeHistoryCoverageReadback(t *testing.T, store driver.Store, archive *dumpformat.Archive, report Report, evidence driver.HistoryImportEvidence) {
	t.Helper()
	complete, images := true, 0
	var commitments []driver.NativeHistoryBoundary
	var histories []NativeHistoryReport
	for _, source := range archive.NativeSourceHistories() {
		identity := dumpformat.SourceIdentity{StoreID: source.StoreID, Project: source.Projects[0]}
		images += len(source.Images)
		histories = append(histories, NativeHistoryReport{Source: identity, Complete: source.Complete, VerifiedCommitCount: uint64(len(source.Images)), Coverage: source.HistoryCoverage})
		if source.HistoryCoverage != nil {
			complete = false
			raw := encode(t, source.HistoryCoverage)
			// encode adds a newline; the driver commitment is to JSON object bytes.
			raw = bytes.TrimSpace(raw)
			commitments = append(commitments, driver.NativeHistoryBoundary{SourceStoreID: source.StoreID, Project: identity.Project, VerifiedCommitCount: uint64(len(source.Images)), BoundarySHA256: hashBytes(raw)})
			attachment, present := archive.Member("history/source-attachments/" + identity.Project + ".json")
			if archive.Manifest().SourceIdentityContract == "" {
				attachment, present = archive.Member(dumpformat.SourceHistoryMember)
			}
			if !present || hashBytes(attachment) != source.HistoryCoverage.EventsAttachment.SHA256 {
				t.Fatal("original events attachment bytes lost or rewritten")
			}
			var original dumpformat.SourceHistory
			decode(t, attachment, &original)
			if uint64(len(original.Events)) != source.HistoryCoverage.EventsAttachment.Rows || len(original.Events) != len(source.Current["events"].Rows) || original.Range.Complete || original.Range.Status != "unverified" || original.Range.Reason != "native-history-boundary" {
				t.Fatal("events attachment gained completeness or lost actual rows")
			}
		}
	}
	if complete {
		histories = nil
	}
	if evidence.Complete != complete || evidence.First != 1 || evidence.Head != uint64(images) || !evidence.Floor.IsZero() || len(evidence.Gaps) != 0 || len(evidence.Mapping) != images || !reflect.DeepEqual(evidence.NativeBoundaries, commitments) || !reflect.DeepEqual(report.NativeHistories, histories) {
		t.Fatal("source completeness, independent boundary commitments or exact verified count changed")
	}
	for _, mapped := range evidence.Mapping {
		if len(mapped.CommitIDs) != 1 || len(mapped.EventIDs) != 0 {
			t.Fatal("current events were assigned to immutable source commits")
		}
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	retained, err := reader.Blob(context.Background(), driver.Hash(archive.SHA256()))
	if err != nil || !bytes.Equal(retained, archive.TarBytes()) {
		t.Fatal("committed original boundary archive changed", err)
	}
	rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatal("committed boundary accounting missing", err)
	}
	var accounting nativeFamilyEvidence
	decode(t, rows[0].Body, &accounting)
	if !reflect.DeepEqual(accounting.Histories, histories) {
		t.Fatal("committed boundary/default/omission accounting differs from report")
	}
}

func TestSyntheticNativeBoundaryPublicationAndRecoveryAcrossDrivers(t *testing.T) {
	for _, rightBounded := range []bool{false, true} {
		name := map[bool]string{false: "boundary-and-complete", true: "two-independent-boundaries"}[rightBounded]
		t.Run(name, func(t *testing.T) {
			left := syntheticNativeBoundaryReceiving(t, "pl", "active")
			right := nativeTableArchiveFixture(t, "qa", "done")
			if rightBounded {
				right = syntheticNativeBoundaryReceiving(t, "qa", "done")
			}
			input, err := dumpformat.Merge(bytes.NewReader(left), bytes.NewReader(right))
			if err != nil {
				t.Fatal("merge explicit verified source boundaries", err)
			}
			archive, err := dumpformat.Read(bytes.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					ctx := context.Background()
					store, lease, reopen := openIdentityTarget(t, engine)
					profile := boundNativeTableProfile(t, input, store, lease)
					report, err := RestoreImportWithLease(ctx, bytes.NewReader(input), store, lease, profile)
					if err != nil {
						t.Fatal("publish verified images plus original events", err)
					}
					assertNativeTablesReadback(t, store, profile, report)
					capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
					if err != nil {
						t.Fatal(err)
					}
					plan, err := profile.SourcePlanes.Plan()
					if err != nil {
						t.Fatal(err)
					}
					before := reverseState(t, store)
					checkpoints := map[string]json.RawMessage{}
					for _, provider := range plan.Providers {
						checkpoint, err := capability.LoadCheckpoint(ctx, provider.Namespace)
						if err != nil {
							t.Fatal(err)
						}
						checkpoints[provider.Namespace] = encode(t, checkpoint)
					}
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(input), store, lease, profile); !errors.Is(err, ErrTargetNotEmpty) {
						t.Fatal("populated boundary destination must refuse", err)
					}
					if !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("populated boundary refusal changed core")
					}
					for namespace, saved := range checkpoints {
						checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
						if err != nil || !bytes.Equal(saved, encode(t, checkpoint)) {
							t.Fatal("populated boundary refusal changed provider checkpoint", err)
						}
					}
					store, lease = reopen()
					bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
					if err != nil || !found || !reflect.DeepEqual(report.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(report.Receipt, recovered.Receipt) || bound.HistoryTime != profile.HistoryTime || !bytes.Equal(recovered.Archive.TarBytes(), input) {
						t.Fatal("boundary reports/receipt/original archive lost on recovery", err)
					}
					if len(recovered.NativeHistories) != len(archive.NativeSourceHistories()) {
						t.Fatal("per-source completeness report missing")
					}
					assertNativeTablesReadback(t, store, bound, recovered)
				})
			}
		})
	}
}
