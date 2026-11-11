package plugdump

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func boundaryFixture(t *testing.T) (Manifest, NativeHistory, map[string][]byte) {
	t.Helper()
	s, inventory := nativeHistoryFixtureForTest(t)
	missing := s.log.Commits[0].ID
	s.log.Commits = s.log.Commits[1:]
	s.log.Boundaries = []NativeHistoryBoundary{{CommitID: missing, Reason: "ghost_parent"}}
	if err := locateHistoryBoundaries(&s.log); err != nil {
		t.Fatal(err)
	}
	data := buildFixture(t, s, inventory)
	m, h := nativeCapture(t, data)
	return m, h, archiveMembers(t, data)
}

func TestNativeHistoryBoundaryRetainsVerifiedImagesAndExactEventsAttachment(t *testing.T) {
	m, h, members := boundaryFixture(t)
	c := h.HistoryCoverage
	if c == nil || c.VerifiedCommitCount != 3 || h.Complete || len(h.Gaps) != 0 || len(h.Images) != 3 || len(c.Boundaries) != 1 || c.Boundaries[0].CommitOrdinalFromHead != 4 || c.Boundaries[0].CommitSchemaVersion != nil {
		t.Fatal("verified boundary census differs")
	}
	if c.EventsAttachment.SHA256 != digest(members[SourceHistoryMember]) || c.EventsAttachment.Member != SourceHistoryMember || c.EventsAttachment.Rows != uint64(len(h.Current["events"].Rows)) {
		t.Fatal("original events attachment not bound")
	}
	if err := validateSourceHistory(m, members); err != nil {
		t.Fatal(err)
	}
	if err := validateNativeHistory(m, members); err != nil {
		t.Fatal(err)
	}
	var portable SourceHistory
	if err := decodeJSON(members[SourceHistoryMember], &portable); err != nil {
		t.Fatal(err)
	}
	if portable.Range.Complete || portable.Range.Reason != "native-history-boundary" || len(portable.Range.Frames) != 0 || !sameEventImage(portable.Events, h.Current["events"].Rows) {
		t.Fatal("events fabricated into portable commit replay")
	}
}

func TestNativeBoundaryExtendedDAGOrdinalsAndSharedFrontier(t *testing.T) {
	id := func(s string) string { return strings.Repeat(s, 32) }
	a, b, c, d, e := id("a"), id("b"), id("c"), id("d"), id("e")
	log := SourceCommitLog{Head: e, Commits: []SourceCommit{{ID: e, Parents: []string{c, d}, Metadata: Row{}}, {ID: c, Parents: []string{a, b}, Metadata: Row{}}, {ID: d, Parents: []string{a}, Metadata: Row{}}}, Boundaries: []NativeHistoryBoundary{{CommitID: a, Reason: "ghost_parent"}, {CommitID: b, Reason: "missing_root_chunk"}}}
	if err := locateHistoryBoundaries(&log); err != nil {
		t.Fatal(err)
	}
	ordered, ordinals, err := boundaryCommitOrder(log, true)
	if err != nil || len(ordered) != 3 || ordinals[e] != 1 || ordinals[a] != 5 || ordinals[b] != 4 || !equalJSON(log.Boundaries[0].ChildCommitIDs, []string{c, d}) {
		t.Fatal("plural/shared frontier canonical coordinates differ")
	}
	if _, err := nativeCommitOrder(log); err == nil {
		t.Fatal("legacy verifier accepted external parents")
	}
	log.Boundaries[0].ChildCommitIDs = []string{c}
	if _, err := historyCommitOrder(log); err == nil {
		t.Fatal("one omitted frontier edge accepted")
	}
}

func TestNativeHistoryBoundaryRejectsPresentHistoryAndAttachmentContradictions(t *testing.T) {
	for _, scenario := range []string{"complete", "no-coverage", "empty-boundaries", "zero-count", "wrong-count", "missing-image", "wrong-ordinal", "wrong-child", "internal-frontier", "unknown-reason", "ghost-schema", "zero-sql", "duplicate-sql", "unordered-sql", "event-hash", "event-count", "event-member", "event-cells", "corrupt-present-row", "portable-complete", "cycle", "disconnected", "duplicate-commit"} {
		t.Run(scenario, func(t *testing.T) {
			m, h, members := boundaryFixture(t)
			c := h.HistoryCoverage
			switch scenario {
			case "complete":
				h.Complete = true
			case "no-coverage":
				h.HistoryCoverage = nil
			case "empty-boundaries":
				c.Boundaries = []NativeHistoryBoundary{}
			case "zero-count":
				c.VerifiedCommitCount = 0
			case "wrong-count":
				c.VerifiedCommitCount++
			case "missing-image":
				h.Images = h.Images[1:]
			case "wrong-ordinal":
				c.Boundaries[0].CommitOrdinalFromHead = 2
			case "wrong-child":
				c.Boundaries[0].ChildCommitIDs = []string{h.Head}
			case "internal-frontier":
				c.Boundaries[0].CommitID = h.Head
			case "unknown-reason":
				c.Boundaries[0].Reason = "text-guessed"
			case "ghost-schema":
				v := uint64(66)
				c.Boundaries[0].CommitSchemaVersion = &v
			case "zero-sql":
				c.Boundaries[0].SQLDoltCodes = []uint16{0}
			case "duplicate-sql":
				c.Boundaries[0].SQLDoltCodes = []uint16{1105, 1105}
			case "unordered-sql":
				c.Boundaries[0].SQLDoltCodes = []uint16{1105, 1040}
			case "event-hash":
				c.EventsAttachment.SHA256 = strings.Repeat("0", 64)
			case "event-count":
				c.EventsAttachment.Rows++
			case "event-member":
				c.EventsAttachment.Member = "records/events.ndjson"
			case "event-cells":
				h.Current["events"].Rows[0]["actor"] = "changed public event"
			case "corrupt-present-row":
				h.Images[0].Tables["issues"].Rows[0]["invented_column"] = true
			case "portable-complete":
				var p SourceHistory
				if err := decodeJSON(members[SourceHistoryMember], &p); err != nil {
					t.Fatal(err)
				}
				p.Range.Complete = true
				members[SourceHistoryMember] = mustJSON(t, p)
				c.EventsAttachment.SHA256 = digest(members[SourceHistoryMember])
			case "cycle":
				h.Commits[0].Parents = []string{h.Head}
			case "disconnected":
				h.Commits = append(h.Commits, SourceCommit{ID: strings.Repeat("v", 32), Parents: []string{}, Metadata: Row{}})
			case "duplicate-commit":
				h.Commits = append(h.Commits, h.Commits[0])
			}
			members[NativeHistoryMember] = mustJSON(t, h)
			if err := validateNativeHistory(m, members); err == nil {
				t.Fatal("contradictory boundary archive accepted")
			}
		})
	}
}

func TestNativeHistoryBoundaryWireRejectsMissingNullUnknownAndNoncanonicalFields(t *testing.T) {
	_, h, _ := boundaryFixture(t)
	original := mustJSON(t, h)
	for _, scenario := range []string{"null-coverage", "missing-rows", "null-rows", "unknown-coverage", "unknown-boundary", "noncanonical-coverage", "null-schema"} {
		t.Run(scenario, func(t *testing.T) {
			var wire map[string]any
			d := json.NewDecoder(bytes.NewReader(original))
			d.UseNumber()
			if err := d.Decode(&wire); err != nil {
				t.Fatal(err)
			}
			c := wire["history_coverage"].(map[string]any)
			switch scenario {
			case "null-coverage":
				wire["history_coverage"] = nil
			case "missing-rows":
				delete(c["events_attachment"].(map[string]any), "rows")
			case "null-rows":
				c["events_attachment"].(map[string]any)["rows"] = nil
			case "unknown-coverage":
				c["verified"] = true
			case "unknown-boundary":
				c["boundaries"].([]any)[0].(map[string]any)["guess"] = "private"
			case "noncanonical-coverage":
				delete(wire, "history_coverage")
				wire["History_Coverage"] = c
			case "null-schema":
				c["boundaries"].([]any)[0].(map[string]any)["commit_schema_version"] = nil
			}
			if err := validateHistoryCoverageShape(mustJSON(t, wire)); err == nil {
				t.Fatal("noncanonical coverage accepted")
			}
		})
	}
}
