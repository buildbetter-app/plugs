package dumpformat

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func sourceHistoryFixture(t *testing.T, partial bool) (*mergeFixture, SourceHistory) {
	t.Helper()
	f := newMergeFixture(t, "pl")
	records := []Record{}
	for _, kind := range historyKinds {
		records = append(records, f.records[kind]...)
	}
	h := SourceHistory{Format: SourceHistoryFormat, Commits: []SourceCommit{
		{ID: strings.Repeat("a", 32), Parents: []string{}, Metadata: Row{"message": "synthetic initial commit"}},
		{ID: strings.Repeat("b", 32), Parents: []string{strings.Repeat("a", 32)}, Metadata: Row{"message": "synthetic update"}},
	}, Events: []Row{}, Range: SourceHistoryRange{Status: "verified", Reason: "", Complete: true, First: 1, Head: 2,
		Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{
			{Sequence: 1, CommitID: strings.Repeat("a", 32), Records: records, Events: []Row{}},
			{Sequence: 2, CommitID: strings.Repeat("b", 32), Records: records, Events: []Row{}},
		}}}
	if partial {
		h.Range.First, h.Range.Complete = 2, false
		h.Range.Baseline = records
		h.Range.Frames = h.Range.Frames[1:]
		h.Range.Gaps = []HistoryGap{{First: 1, Last: 1, Reason: "not-retained"}}
	}
	return f, h
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
	if _, err := MergeArchives(a); err == nil {
		t.Fatal("merge silently discarded source history")
	}
}
