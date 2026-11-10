package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// This is a declared synthetic transport fixture. Genuine unavailable-source
// evidence must come from the separately retained producer public fixture.
func syntheticNativeBoundary(t *testing.T, project string, branched bool) []byte {
	t.Helper()
	members := sourcePlaneReaderMembers(t, nativeSyntheticFixture(t, project))
	var h NativeHistory
	var portable SourceHistory
	if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(members[SourceHistoryMember], &portable); err != nil {
		t.Fatal(err)
	}
	h.Commits[0].Parents = []string{strings.Repeat("c", 32)}
	boundaries := []NativeHistoryBoundary{{CommitID: strings.Repeat("c", 32), ChildCommitIDs: []string{h.Commits[0].ID}, CommitOrdinalFromHead: 3, Reason: "ghost_parent"}}
	if branched {
		h.Commits[1].Parents = append(h.Commits[1].Parents, strings.Repeat("d", 32))
		boundaries[0].CommitOrdinalFromHead = 4
		boundaries = append(boundaries, NativeHistoryBoundary{CommitID: strings.Repeat("d", 32), ChildCommitIDs: []string{h.Commits[1].ID}, CommitOrdinalFromHead: 2, Reason: "missing_root_chunk"})
	}
	portable.Commits = h.Commits
	portable.Range = SourceHistoryRange{Status: "unverified", Reason: "native-history-boundary", Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{}}
	members[SourceHistoryMember] = fixtureJSON(t, portable)
	h.Complete = false
	h.HistoryCoverage = &NativeHistoryCoverage{Format: NativeHistoryCoverageFormat, Status: "verified-to-boundary", VerifiedCommitCount: uint64(len(h.Commits)), Boundaries: boundaries, EventsAttachment: NativeEventsAttachment{Member: SourceHistoryMember, SHA256: digest(members[SourceHistoryMember]), Rows: uint64(len(portable.Events))}}
	members[NativeHistoryMember] = fixtureJSON(t, h)
	return nativeFixtureMembers(t, members)
}

func TestNativeHistoryBoundaryGraphAndExactEventsAttachment(t *testing.T) {
	for _, branched := range []bool{false, true} {
		t.Run(map[bool]string{false: "linear", true: "multiple-frontiers"}[branched], func(t *testing.T) {
			raw := syntheticNativeBoundary(t, "pl", branched)
			archive := readMergeArchive(t, raw)
			h := archive.NativeSourceHistories()[0]
			if h.Complete || len(h.Gaps) != 0 || len(h.Images) != 2 || len(h.Commits) != 2 || h.HistoryCoverage.VerifiedCommitCount != 2 || RequireVerifiedNativeHistory(h) != nil {
				t.Fatal("verified source suffix was refused, padded or relabeled complete")
			}
			original, _ := archive.Member(SourceHistoryMember)
			if h.HistoryCoverage.EventsAttachment.SHA256 != digest(original) || h.HistoryCoverage.EventsAttachment.Rows != uint64(len(h.Current["events"].Rows)) {
				t.Fatal("original events attachment bytes or row count changed")
			}
			h.HistoryCoverage.Boundaries[0].ChildCommitIDs[0] = "detached-copy"
			if archive.NativeSourceHistories()[0].HistoryCoverage.Boundaries[0].ChildCommitIDs[0] == "detached-copy" {
				t.Fatal("coverage API exposed archive-owned slices")
			}
		})
	}
}

func TestNativeHistoryBoundaryOptionalSchemaAndClosedReasons(t *testing.T) {
	for _, reason := range []string{"ghost_parent", "missing_root_chunk", "missing_table_chunk", "missing_archive_chunk", "unreadable_schema", "other"} {
		t.Run(reason, func(t *testing.T) {
			raw := editNativeFixture(t, syntheticNativeBoundary(t, "pl", false), func(h *NativeHistory) {
				boundary := &h.HistoryCoverage.Boundaries[0]
				boundary.Reason = reason
				boundary.SQLDoltCodes = []uint16{1105, 1227}
				if reason != "unreadable_schema" && reason != "ghost_parent" {
					observed := uint64(0)
					boundary.CommitSchemaVersion = &observed
				}
			})
			h := readMergeArchive(t, raw).NativeSourceHistories()[0]
			boundary := h.HistoryCoverage.Boundaries[0]
			unreadable := reason == "unreadable_schema" || reason == "ghost_parent"
			if boundary.Reason != reason || !reflect.DeepEqual(boundary.SQLDoltCodes, []uint16{1105, 1227}) || unreadable && boundary.CommitSchemaVersion != nil || !unreadable && (boundary.CommitSchemaVersion == nil || *boundary.CommitSchemaVersion != 0) {
				t.Fatal("boundary schema absence/explicit zero, reason or codes changed")
			}
		})
	}
}

func TestNativeHistoryBoundaryMergeKeepsOriginalAttachmentAndNamespaces(t *testing.T) {
	left, right := syntheticNativeBoundary(t, "pl", true), syntheticNativeBoundary(t, "qa", false)
	merged, err := Merge(bytes.NewReader(right), bytes.NewReader(left))
	if err != nil {
		t.Fatal(err)
	}
	archive := readMergeArchive(t, merged)
	if len(archive.NativeSourceHistories()) != 2 || len(archive.SourceIdentities()) != 2 {
		t.Fatal("boundary source namespaces coalesced")
	}
	for project, original := range map[string][]byte{"pl": left, "qa": right} {
		input := readMergeArchive(t, original)
		want, _ := input.Member(SourceHistoryMember)
		got, present := archive.Member(nativeSourceAttachmentPath(project))
		if !present || !bytes.Equal(want, got) {
			t.Fatal("merge reserialized or lost a source's exact events member", project)
		}
		wantNative, _ := input.Member(NativeHistoryMember)
		gotNative, _ := archive.Member(nativeHistoryPath(project))
		if !bytes.Equal(wantNative, gotNative) {
			t.Fatal("merge rewrote a producer boundary descriptor")
		}
	}
	again, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil || !bytes.Equal(merged, again) {
		t.Fatal("boundary merge ordering is not deterministic", err)
	}
	nested, err := Merge(bytes.NewReader(merged), bytes.NewReader(nativeSyntheticFixture(t, "zz")))
	if err != nil || len(readMergeArchive(t, nested).NativeSourceHistories()) != 3 {
		t.Fatal("nested boundary/complete merge lost a source", err)
	}
}

func TestNativeHistoryBoundaryRefusesPresentHistoryAndAttachmentContradictions(t *testing.T) {
	baseline := syntheticNativeBoundary(t, "pl", true)
	cases := []struct {
		name string
		edit func(*NativeHistory)
	}{
		{"falsely-complete", func(h *NativeHistory) { h.Complete = true }},
		{"missing-descriptor", func(h *NativeHistory) { h.HistoryCoverage = nil }},
		{"zero-verified", func(h *NativeHistory) { h.HistoryCoverage.VerifiedCommitCount = 0 }},
		{"wrong-verified-count", func(h *NativeHistory) { h.HistoryCoverage.VerifiedCommitCount++ }},
		{"missing-present-image", func(h *NativeHistory) { h.Images = h.Images[1:] }},
		{"wrong-image-commit", func(h *NativeHistory) { h.Images[0].CommitID = h.Head }},
		{"cycle", func(h *NativeHistory) { h.Commits[0].Parents = []string{h.Head} }},
		{"uncovered-edge", func(h *NativeHistory) { h.HistoryCoverage.Boundaries = h.HistoryCoverage.Boundaries[:1] }},
		{"invented-edge", func(h *NativeHistory) { h.HistoryCoverage.Boundaries[0].ChildCommitIDs = []string{h.Head} }},
		{"duplicate-child", func(h *NativeHistory) {
			b := &h.HistoryCoverage.Boundaries[0]
			b.ChildCommitIDs = append(b.ChildCommitIDs, b.ChildCommitIDs[0])
		}},
		{"duplicate-frontier", func(h *NativeHistory) {
			h.HistoryCoverage.Boundaries = append(h.HistoryCoverage.Boundaries, h.HistoryCoverage.Boundaries[0])
		}},
		{"wrong-ordinal", func(h *NativeHistory) { h.HistoryCoverage.Boundaries[0].CommitOrdinalFromHead-- }},
		{"internal-frontier", func(h *NativeHistory) { h.HistoryCoverage.Boundaries[0].CommitID = h.Commits[0].ID }},
		{"unknown-reason", func(h *NativeHistory) { h.HistoryCoverage.Boundaries[0].Reason = "probably-gc" }},
		{"ghost-schema-claim", func(h *NativeHistory) {
			version := uint64(66)
			h.HistoryCoverage.Boundaries[0].CommitSchemaVersion = &version
		}},
		{"duplicate-sql-code", func(h *NativeHistory) { h.HistoryCoverage.Boundaries[0].SQLDoltCodes = []uint16{1105, 1105} }},
		{"wrong-events-member", func(h *NativeHistory) { h.HistoryCoverage.EventsAttachment.Member = "records/beads-archive.ndjson" }},
		{"wrong-events-hash", func(h *NativeHistory) { h.HistoryCoverage.EventsAttachment.SHA256 = strings.Repeat("a", 64) }},
		{"wrong-events-count", func(h *NativeHistory) { h.HistoryCoverage.EventsAttachment.Rows++ }},
		{"contradictory-current-cell", func(h *NativeHistory) { h.Current["events"].Rows[0]["actor"] = "contradicted" }},
		{"contradictory-head-cell", func(h *NativeHistory) { h.Images[1].Tables["issues"].Rows[0]["title"] = "contradicted" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Read(bytes.NewReader(editNativeFixture(t, baseline, test.edit))); err == nil {
				t.Fatal("boundary bypassed present-history integrity or events conservation")
			}
		})
	}
	for _, test := range []string{"null-coverage", "missing-count", "unknown-key", "null-schema", "fractional-ordinal", "empty-sql-codes", "missing-events-rows", "null-events-hash"} {
		t.Run(test, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, baseline)
			var object map[string]any
			if err := decodeJSON(members[NativeHistoryMember], &object); err != nil {
				t.Fatal(err)
			}
			coverage := object["history_coverage"].(map[string]any)
			boundary := coverage["boundaries"].([]any)[0].(map[string]any)
			switch test {
			case "null-coverage":
				object["history_coverage"] = nil
			case "missing-count":
				delete(coverage, "verified_commit_count")
			case "unknown-key":
				coverage["inferred_complete"] = true
			case "null-schema":
				boundary["commit_schema_version"] = nil
			case "fractional-ordinal":
				boundary["commit_ordinal_from_head"] = json.Number("3.5")
			case "empty-sql-codes":
				boundary["sql_dolt_codes"] = []any{}
			case "missing-events-rows":
				delete(coverage["events_attachment"].(map[string]any), "rows")
			case "null-events-hash":
				coverage["events_attachment"].(map[string]any)["sha256"] = nil
			}
			members[NativeHistoryMember] = fixtureJSON(t, object)
			if _, err := Read(bytes.NewReader(nativeFixtureMembers(t, members))); err == nil {
				t.Fatal("noncanonical boundary wire admitted")
			}
		})
	}
}

func TestNativeHistoryBoundaryMergeRefusesSubstitutedEventsMember(t *testing.T) {
	left, right := syntheticNativeBoundary(t, "pl", false), syntheticNativeBoundary(t, "qa", false)
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"substituted", "orphan", "omitted"} {
		t.Run(fault, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, merged)
			switch fault {
			case "substituted":
				members[nativeSourceAttachmentPath("pl")] = members[nativeSourceAttachmentPath("qa")]
			case "orphan":
				members[nativeSourceAttachmentPath("xx")] = members[nativeSourceAttachmentPath("pl")]
			case "omitted":
				delete(members, nativeSourceAttachmentPath("pl"))
			}
			if _, err := Read(bytes.NewReader(nativeFixtureMembers(t, members))); err == nil {
				t.Fatal("source-specific original events attachment was not authenticated")
			}
		})
	}
	original := readMergeArchive(t, left).NativeSourceHistories()[0]
	detached := readMergeArchive(t, left).NativeSourceHistories()[0]
	if !reflect.DeepEqual(original.HistoryCoverage, detached.HistoryCoverage) {
		t.Fatal("coverage extraction changed source facts")
	}
}
