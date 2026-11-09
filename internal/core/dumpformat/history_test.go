package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
)

func sourceHistoryFixture(t *testing.T, partial bool) (*mergeFixture, SourceHistory) {
	t.Helper()
	f := newMergeFixture(t, "pl")
	h := sourceHistoryForFixture(t, f, strings.Repeat("a", 32), strings.Repeat("b", 32), partial)
	return f, h
}

func sourceHistoryForFixture(t *testing.T, f *mergeFixture, first, second string, partial bool) SourceHistory {
	t.Helper()
	records := []Record{}
	for _, kind := range historyKinds {
		records = append(records, f.records[kind]...)
	}
	h := SourceHistory{Format: SourceHistoryFormat, Commits: []SourceCommit{
		{ID: first, Parents: []string{}, Metadata: Row{"message": "synthetic initial commit"}},
		{ID: second, Parents: []string{first}, Metadata: Row{"message": "synthetic update"}},
	}, Events: []Row{}, Range: SourceHistoryRange{Status: "verified", Reason: "", Complete: true, First: 1, Head: 2,
		Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{
			{Sequence: 1, CommitID: first, Records: records, Events: []Row{}},
			{Sequence: 2, CommitID: second, Records: records, Events: []Row{}},
		}}}
	if partial {
		h.Range.First, h.Range.Complete = 2, false
		h.Range.Baseline = records
		h.Range.Frames = h.Range.Frames[1:]
		h.Range.Gaps = []HistoryGap{{First: 1, Last: 1, Reason: "not-retained"}}
	}
	return h
}

func withSourceHistory(t *testing.T, f *mergeFixture, h SourceHistory) []byte {
	t.Helper()
	return f.bytes(t, func(m *Manifest, files map[string][]byte) {
		if h.Range.Status == "verified" && h.Range.Complete {
			m.History = "verified"
		}
		files[SourceHistoryMember] = fixtureJSON(t, h)
		m.Files[SourceHistoryMember] = File{Size: int64(len(files[SourceHistoryMember])), SHA256: digest(files[SourceHistoryMember])}
	})
}

func TestSourceHistoryRequiresCheckedImagesAndExactHead(t *testing.T) {
	for _, partial := range []bool{false, true} {
		f, h := sourceHistoryFixture(t, partial)
		a, err := Read(bytes.NewReader(withSourceHistory(t, f, h)))
		if err != nil {
			t.Fatal(err)
		}
		got, ok := a.SourceHistory()
		if !ok || got.Range.First != h.Range.First {
			t.Fatal("verified range missing")
		}
		got.Range.Frames[0].Records[0].Source.Fields["title"] = "caller mutation"
		fresh, _ := a.SourceHistory()
		if fresh.Range.Frames[0].Records[0].Source.Fields["title"] == "caller mutation" {
			t.Fatal("history accessor aliases validated data")
		}
		image, err := a.HistoryImage(h.Range.Head)
		if err != nil || len(image.Records("bead")) != len(a.Records("bead")) {
			t.Fatalf("checked image: %v", err)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*SourceHistory)
	}{
		{"broken-parent", func(h *SourceHistory) { h.Commits[1].Parents[0] = strings.Repeat("c", 32) }},
		{"merge", func(h *SourceHistory) { h.Commits[1].Parents = append(h.Commits[1].Parents, strings.Repeat("c", 32)) }},
		{"missing-image", func(h *SourceHistory) { h.Range.Frames = h.Range.Frames[:1] }},
		{"invented-commit", func(h *SourceHistory) { h.Range.Frames[1].CommitID = strings.Repeat("c", 32) }},
		{"head-diverges", func(h *SourceHistory) { h.Range.Frames[1].Records = []Record{} }},
		{"unknown-event", func(h *SourceHistory) { h.Events = []Row{{"id": json.Number("1")}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, h := sourceHistoryFixture(t, false)
			tc.change(&h)
			if _, err := Read(bytes.NewReader(withSourceHistory(t, f, h))); err == nil {
				t.Fatal("unchecked source history accepted")
			}
		})
	}
}

func TestSourceHistoryDoesNotPromoteRawCommitsToVerifiedReplay(t *testing.T) {
	f, h := sourceHistoryFixture(t, false)
	h.Range = SourceHistoryRange{Status: "unverified", Reason: "historical schema is not representable", Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{}}
	a, err := Read(bytes.NewReader(withSourceHistory(t, f, h)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.HistoryImage(1); err == nil {
		t.Fatal("raw source commits fabricated a replay image")
	}
	merged, err := MergeArchives(a)
	var refusal IncompleteHistoryError
	if !errors.As(err, &refusal) || merged != nil || refusal.Project != "pl" || refusal.Reason != h.Range.Reason {
		t.Fatalf("incomplete source merge must refuse without promoting history: %v", err)
	}
	retained, ok := a.SourceHistory()
	if !ok || retained.Range.Status != "unverified" {
		t.Fatal("merge mutated unverified source evidence")
	}
}

func TestMergePreservesDisjointProjectHistories(t *testing.T) {
	aFixture := newMergeFixture(t, "pl")
	bFixture := newMergeFixture(t, "qa")
	aHistory := sourceHistoryForFixture(t, aFixture, strings.Repeat("a", 32), strings.Repeat("b", 32), false)
	bHistory := sourceHistoryForFixture(t, bFixture, strings.Repeat("c", 32), strings.Repeat("d", 32), false)
	left := withSourceHistory(t, aFixture, aHistory)
	right := withSourceHistory(t, bFixture, bHistory)
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	archive := readMergeArchive(t, merged)
	history, ok := archive.SourceHistory()
	if !ok || len(history.ProjectHistories) != 2 {
		t.Fatalf("merged project histories: ok=%t count=%d", ok, len(history.ProjectHistories))
	}
	for _, project := range []string{"pl", "qa"} {
		image, err := archive.HistoryImageForProject(project, 2)
		if err != nil {
			t.Fatalf("project %s image: %v", project, err)
		}
		if len(image.Records("bead")) != 3 || len(image.Records("config")) == 0 {
			t.Fatalf("project %s history image lost source rows", project)
		}
	}
	if history.ProjectHistories[0].Project != "pl" || history.ProjectHistories[1].Project != "qa" {
		t.Fatalf("project history order is not deterministic: %#v", history.ProjectHistories)
	}
}

func TestMergePreservesOverlappingSourceRevisionIDsAcrossProjects(t *testing.T) {
	aFixture := newMergeFixture(t, "pl")
	bFixture := newMergeFixture(t, "qa")
	aHistory := sourceHistoryForFixture(t, aFixture, strings.Repeat("a", 32), strings.Repeat("b", 32), false)
	bHistory := sourceHistoryForFixture(t, bFixture, strings.Repeat("a", 32), strings.Repeat("c", 32), false)
	left := withSourceHistory(t, aFixture, aHistory)
	right := withSourceHistory(t, bFixture, bHistory)
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	history, ok := readMergeArchive(t, merged).SourceHistory()
	if !ok || len(history.ProjectHistories) != 2 || history.ProjectHistories[0].Commits[0].ID != strings.Repeat("a", 32) || history.ProjectHistories[1].Commits[0].ID != strings.Repeat("a", 32) || history.ProjectHistories[0].SourceStoreID == history.ProjectHistories[1].SourceStoreID {
		t.Fatal("colliding raw revisions lost their independent source stores")
	}
}

func TestSourceHistoryChecksTypeDirectivesInEveryImage(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, policy := range []string{"unused-earlier", "covered-earlier", "missing", "wrong-issue", "wrong-type"} {
			name := fmt.Sprintf("partial=%t/%s", partial, policy)
			t.Run(name, func(t *testing.T) {
				f, original := sourceHistoryFixture(t, partial)
				var h SourceHistory
				if err := decodeJSON(fixtureJSON(t, original), &h); err != nil {
					t.Fatal(err)
				}
				setType := func(rows []Record, id, issueType string) {
					t.Helper()
					for i := range rows {
						if rows[i].Kind == "bead" && rows[i].ID == id {
							rows[i].Source.Fields["issue_type"] = issueType
							rows[i].Body["source_type"] = issueType
							return
						}
					}
					t.Fatalf("missing fixture bead %s", id)
				}
				firstID, secondID := f.records["bead"][0].ID, f.records["bead"][1].ID
				earlier := h.Range.Frames[0].Records
				if partial {
					earlier = h.Range.Baseline
				}
				if policy != "missing" {
					setType(f.records["bead"], firstID, "decision")
					setType(h.Range.Frames[len(h.Range.Frames)-1].Records, firstID, "decision")
					f.manifest.Ontology = []OntologyDirective{{ID: "decision", Kind: "map_type", Target: OntologyTarget{Project: "pl", Type: "decision", ID: firstID}, From: "decision", To: "issue"}}
					f.manifest.Verification["ontology"] = explicitOntologyVerification
				}
				switch policy {
				case "covered-earlier", "missing":
					setType(earlier, firstID, "decision")
				case "wrong-issue":
					setType(earlier, secondID, "decision")
				case "wrong-type":
					setType(earlier, firstID, "uncovered-source-type")
				}
				a, err := Read(bytes.NewReader(withSourceHistory(t, f, h)))
				if policy == "unused-earlier" || policy == "covered-earlier" {
					if err != nil {
						t.Fatal(err)
					}
					sequence := uint64(1)
					if partial {
						sequence = h.Range.First - 1
					}
					if _, err := a.HistoryImage(sequence); err != nil {
						t.Fatal(err)
					}
					return
				}
				var refusal ErrSourceHistory
				if !errors.As(err, &refusal) || refusal.Reason != "unmappable" {
					t.Fatalf("historical type escaped directive scope: %v", err)
				}
			})
		}
	}
}

func TestProjectHistoryMembershipDoesNotInferProjectFromMemoryName(t *testing.T) {
	var inputs []io.Reader
	for i, prefix := range []string{"pl", "qa"} {
		fixture := newMergeFixture(t, prefix)
		key := "notes-" + strconv.Itoa(i)
		fixture.records["memory"][0].ID = key
		fixture.records["memory"][0].Body["key"] = key
		fixture.records["memory"][0].Source.Fields["key"] = "kv.memory." + key
		history := sourceHistoryForFixture(t, fixture, strings.Repeat(string(rune('a'+2*i)), 32), strings.Repeat(string(rune('b'+2*i)), 32), false)
		inputs = append(inputs, bytes.NewReader(withSourceHistory(t, fixture, history)))
	}
	merged, err := Merge(inputs...)
	if err != nil {
		t.Fatal(err)
	}
	archive := readMergeArchive(t, merged)
	for i, project := range []string{"pl", "qa"} {
		image, err := archive.HistoryImageForProject(project, 2)
		if err != nil {
			t.Fatal(err)
		}
		rows := image.Records("memory")
		if len(rows) != 1 || rows[0].ID != "notes-"+strconv.Itoa(i) {
			t.Fatal("memory crossed the explicit history project boundary")
		}
	}
}

func TestMergeRetainsTypeDirectivesFromEachProject(t *testing.T) {
	var inputs []io.Reader
	for i, prefix := range []string{"pl", "qa"} {
		fixture := newMergeFixture(t, prefix)
		row := &fixture.records["bead"][0]
		row.Source.Fields["issue_type"], row.Body["source_type"] = "decision", "decision"
		fixture.manifest.Ontology = []OntologyDirective{{ID: prefix + "-decision", Kind: "map_type", Target: OntologyTarget{Project: prefix, Type: "decision", ID: row.ID}, From: "decision", To: "issue"}}
		fixture.manifest.Verification["ontology"] = explicitOntologyVerification
		history := sourceHistoryForFixture(t, fixture, strings.Repeat(string(rune('a'+2*i)), 32), strings.Repeat(string(rune('b'+2*i)), 32), false)
		inputs = append(inputs, bytes.NewReader(withSourceHistory(t, fixture, history)))
	}
	merged, err := Merge(inputs...)
	if err != nil {
		t.Fatal(err)
	}
	archive := readMergeArchive(t, merged)
	if len(archive.Manifest().Ontology) != 2 {
		t.Fatal("merge discarded a project's explicit type directive")
	}
	for _, project := range []string{"pl", "qa"} {
		if _, err := archive.HistoryImageForProject(project, 1); err != nil {
			t.Fatal(err)
		}
	}
}
