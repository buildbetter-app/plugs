package plugdump

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type historyFixtureSource struct {
	fixtureSource
	log    SourceCommitLog
	images map[string]map[string]Table
	reads  []string
}

func (s *historyFixtureSource) ReadSourceCommits(context.Context) (SourceCommitLog, error) {
	return s.log, nil
}

func (s *historyFixtureSource) ReadSourceImage(_ context.Context, id string) (map[string]Table, error) {
	s.reads = append(s.reads, id)
	image, ok := s.images[id]
	if !ok {
		return nil, fmt.Errorf("historical image unavailable")
	}
	return image, nil
}

func historyFixture(t *testing.T) (*historyFixtureSource, Inventory) {
	t.Helper()
	head, inventory := fixture(t)
	s := &historyFixtureSource{fixtureSource: head, images: map[string]map[string]Table{}, log: SourceCommitLog{Clean: true}}
	for i := 1; i <= 4; i++ {
		id := strings.Repeat(string(rune('a'+i-1)), 32)
		parents := []string{}
		if i > 1 {
			parents = []string{s.log.Commits[i-2].ID}
		}
		s.log.Commits = append(s.log.Commits, SourceCommit{ID: id, Parents: parents, Metadata: Row{"message": "synthetic commit " + fmt.Sprint(i), "committer": "source author is provenance"}})
		var image map[string]Table
		data, err := json.Marshal(head)
		if err != nil {
			t.Fatal(err)
		}
		if err = decodeJSON(data, &image); err != nil {
			t.Fatal(err)
		}
		if i < 4 {
			image["issues"].Rows[0]["title"] = "historical title " + fmt.Sprint(i)
			image["events"] = Table{Columns: image["events"].Columns, Rows: []Row{}}
		}
		s.images[id] = image
	}
	s.log.Head = s.log.Commits[3].ID
	return s, inventory
}

func sourceCapture(t *testing.T, data []byte) (Manifest, SourceHistory) {
	t.Helper()
	m, err := Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var h SourceHistory
	if err := decodeJSON(archiveMembers(t, data)[SourceHistoryMember], &h); err != nil {
		t.Fatalf("missing/invalid emitted source history: %v", err)
	}
	return m, h
}

func TestExportSourceHistoryFullUsesActualCommitImages(t *testing.T) {
	s, inventory := historyFixture(t)
	m, h := sourceCapture(t, buildFixture(t, s, inventory))
	if m.History != "verified" || !h.Range.Complete || h.Range.Status != "verified" || h.Range.First != 1 || h.Range.Head != 4 || len(h.Range.Frames) != 4 {
		t.Fatalf("full range: %+v", h.Range)
	}
	if len(h.Commits) != 4 || len(h.Events) != len(s.fixtureSource["events"].Rows) || len(s.reads) != 4 {
		t.Fatal("source commit/event census or image read missing")
	}
	for i, frame := range h.Range.Frames {
		if frame.CommitID != s.log.Commits[i].ID || frame.Sequence != uint64(i+1) {
			t.Fatalf("invented source sequence: %+v", frame)
		}
	}
	if !bytes.Contains(archiveMembers(t, buildFixture(t, s, inventory))[SourceHistoryMember], []byte("historical title 1")) {
		t.Fatal("historical image filled from current state")
	}
}

func TestExportSourceHistoryPartialRecordsExactPrefixGaps(t *testing.T) {
	s, inventory := historyFixture(t)
	delete(s.images, s.log.Commits[0].ID)
	m, h := sourceCapture(t, buildFixture(t, s, inventory))
	if m.History != "available-unverified" || h.Range.Complete || h.Range.Status != "verified" || h.Range.First != 3 || h.Range.Head != 4 || len(h.Range.Baseline) == 0 || len(h.Range.Frames) != 2 {
		t.Fatalf("partial suffix/baseline: %+v", h.Range)
	}
	if len(h.Range.Gaps) != 2 || h.Range.Gaps[0] != (HistoryGap{First: 1, Last: 1, Reason: "unavailable"}) || h.Range.Gaps[1] != (HistoryGap{First: 2, Last: 2, Reason: "not-retained"}) {
		t.Fatalf("exact gaps: %+v", h.Range.Gaps)
	}
}

func TestExportSourceHistoryCannotPromoteUnprovenRanges(t *testing.T) {
	for _, scenario := range []string{"dirty", "merge", "missing-parent", "wrong-head", "no-baseline", "event-rewrite"} {
		t.Run(scenario, func(t *testing.T) {
			s, inventory := historyFixture(t)
			switch scenario {
			case "dirty":
				s.log.Clean = false
			case "merge":
				s.log.Commits[2].Parents = append(s.log.Commits[2].Parents, s.log.Commits[0].ID)
			case "missing-parent":
				s.log.Commits[2].Parents = []string{strings.Repeat("v", 32)}
			case "wrong-head":
				s.images[s.log.Head]["issues"].Rows[0]["title"] = "not the actual exported head"
			case "no-baseline":
				for i := 0; i < 3; i++ {
					delete(s.images, s.log.Commits[i].ID)
				}
			case "event-rewrite":
				events := s.images[s.log.Commits[2].ID]["events"]
				events.Rows = []Row{}
				for _, event := range s.fixtureSource["events"].Rows {
					copy := Row{}
					for k, v := range event {
						copy[k] = v
					}
					copy["actor"] = "a rewritten audit event"
					events.Rows = append(events.Rows, copy)
				}
				s.images[s.log.Commits[2].ID]["events"] = events
			}
			m, h := sourceCapture(t, buildFixture(t, s, inventory))
			if m.History == "verified" || h.Range.Status != "unverified" || h.Range.Reason == "" || h.Range.First != 0 || len(h.Range.Frames) != 0 {
				t.Fatalf("unproven source was promoted: %+v", h.Range)
			}
			if len(h.Commits) != 4 || len(h.Events) != len(s.fixtureSource["events"].Rows) {
				t.Fatal("raw source evidence discarded on replay refusal")
			}
		})
	}
}
