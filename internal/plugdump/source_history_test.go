package plugdump

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestExportOpaqueIssueFieldsSurviveEveryHistoryImage(t *testing.T) {
	s, inventory := historyFixture(t)
	opaque := Row{
		"agent_state": "waiting\x00opaque", "await_id": "approval/byte-for-byte", "await_type": "human",
		"ephemeral": json.Number("1"), "event_kind": nil, "hook_bead": "demo-8", "is_template": json.Number("1"),
		"mol_type": "formula", "payload": map[string]any{"n": json.Number("9007199254740993"), "null": nil},
		"role_bead": "demo-7", "role_type": "agent", "target": "demo-8", "timeout_ns": json.Number("9007199254740993"),
		"waiters": []any{"demo-8", nil}, "wisp_type": "run", "work_type": "workflow",
	}
	apply := func(tables map[string]Table) {
		for _, row := range tables["issues"].Rows {
			for name, value := range opaque {
				row[name] = value
			}
		}
	}
	apply(s.fixtureSource)
	for _, tables := range s.images {
		apply(tables)
	}
	_, h := sourceCapture(t, buildFixture(t, s, inventory))
	if h.Range.Status != "verified" || !h.Range.Complete {
		t.Fatal("opaque data blocked source proof")
	}
	for _, frame := range h.Range.Frames {
		for _, record := range frame.Records {
			if record.Kind != "bead" {
				continue
			}
			for field, want := range opaque {
				if !equalJSON(record.Source.Fields[field], want) {
					t.Fatalf("sequence %d opaque field %s changed", frame.Sequence, field)
				}
			}
		}
	}
}

type movingHistorySource struct {
	*historyFixtureSource
	final    SourceCommitLog
	censuses int
}

func (s *movingHistorySource) ReadSourceCommits(context.Context) (SourceCommitLog, error) {
	s.censuses++
	if s.censuses == 1 {
		return s.log, nil
	}
	return s.final, nil
}

func TestExportSourceHistoryRejectsMovingCensus(t *testing.T) {
	for _, change := range []string{"head", "dirty", "parent", "metadata"} {
		t.Run(change, func(t *testing.T) {
			fixture, inventory := historyFixture(t)
			data, err := json.Marshal(fixture.log)
			if err != nil {
				t.Fatal(err)
			}
			var final SourceCommitLog
			if err := decodeJSON(data, &final); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "head":
				final.Head = final.Commits[len(final.Commits)-2].ID
			case "dirty":
				final.Clean = false
			case "parent":
				final.Commits[2].Parents = []string{strings.Repeat("v", 32)}
			case "metadata":
				final.Commits[2].Metadata["message"] = "changed census"
			}
			s := &movingHistorySource{historyFixtureSource: fixture, final: final}
			m, h := sourceCapture(t, buildFixture(t, s, inventory))
			if m.History != "available-unverified" || h.Range.Status != "unverified" || h.Range.Reason != "source-census-changed" || len(h.Range.Frames) != 0 {
				t.Fatalf("moving source census became replayable: status=%s reason=%s first=%d head=%d frames=%d", h.Range.Status, h.Range.Reason, h.Range.First, h.Range.Head, len(h.Range.Frames))
			}
		})
	}
}

func TestExportSourceHistoryPreservesScopedHeadTypeDirectives(t *testing.T) {
	for _, earlier := range []string{"builtin", "covered", "wrong-issue"} {
		t.Run(earlier, func(t *testing.T) {
			s, inventory := historyFixture(t)
			s.fixtureSource["issues"].Rows[0]["issue_type"] = "decision"
			s.images[s.log.Head]["issues"].Rows[0]["issue_type"] = "decision"
			inventory.Ontology = []OntologyDirective{{ID: "preserve-decision", Kind: "map_type", Target: OntologyTarget{Project: "demo", Type: "decision", ID: "demo-7"}, From: "decision", To: "issue"}}
			if earlier == "covered" {
				for _, image := range s.images {
					image["issues"].Rows[0]["issue_type"] = "decision"
				}
			}
			if earlier == "wrong-issue" {
				s.images[s.log.Commits[2].ID]["issues"].Rows[1]["issue_type"] = "decision"
			}
			_, h := sourceCapture(t, buildFixture(t, s, inventory))
			if earlier == "wrong-issue" {
				if h.Range.Status != "unverified" || h.Range.Complete {
					t.Fatal("historical issue borrowed a directive for another issue")
				}
				return
			}
			if h.Range.Status != "verified" || !h.Range.Complete || len(h.Range.Frames) != 4 {
				t.Fatalf("valid scoped type history lost: status=%s reason=%s first=%d frames=%d", h.Range.Status, h.Range.Reason, h.Range.First, len(h.Range.Frames))
			}
		})
	}
}

func TestSourceHistoryValidationChecksTypeScopeInFramesAndBaseline(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, scope := range []string{"missing", "wrong-issue", "covered"} {
			t.Run(fmt.Sprintf("partial=%t/%s", partial, scope), func(t *testing.T) {
				s, inventory := historyFixture(t)
				if partial {
					delete(s.images, s.log.Commits[0].ID)
				}
				if scope != "missing" {
					s.fixtureSource["issues"].Rows[0]["issue_type"] = "decision"
					s.images[s.log.Head]["issues"].Rows[0]["issue_type"] = "decision"
					inventory.Ontology = []OntologyDirective{{ID: "decision", Kind: "map_type", Target: OntologyTarget{Project: "demo", Type: "decision", ID: "demo-7"}, From: "decision", To: "issue"}}
				}
				data := buildFixture(t, s, inventory)
				manifest, h := sourceCapture(t, data)
				rows := h.Range.Frames[0].Records
				if partial {
					rows = h.Range.Baseline
				}
				id := "demo-7"
				if scope == "wrong-issue" {
					id = "demo-8"
				}
				for i := range rows {
					if rows[i].Kind == "bead" && rows[i].ID == id {
						rows[i].Source.Fields["issue_type"] = "decision"
						rows[i].Body["source_type"] = "decision"
					}
				}
				members := archiveMembers(t, data)
				var err error
				members[SourceHistoryMember], err = marshal(h)
				if err != nil {
					t.Fatal(err)
				}
				err = validateSourceHistory(manifest, members)
				if scope == "covered" {
					if err != nil {
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

func TestSourceParentCensusRequiresExplicitRootAndValidParents(t *testing.T) {
	root, child := strings.Repeat("a", 32), strings.Repeat("b", 32)
	rootRow := Row{"commit_hash": root, "parent_hash": nil}
	edge := Row{"commit_hash": child, "parent_hash": root}
	parents, err := sourceParentCensus([]Row{edge, rootRow})
	if err != nil || len(parents) != 2 || len(parents[root]) != 0 || len(parents[child]) != 1 || parents[child][0] != root {
		t.Fatalf("explicit pinned Dolt root was not retained: %v", err)
	}
	for name, rows := range map[string][]Row{
		"empty-parent":   {{"commit_hash": root, "parent_hash": ""}},
		"invalid-parent": {{"commit_hash": child, "parent_hash": "not-a-hash"}},
		"duplicate-root": {rootRow, rootRow},
		"duplicate-edge": {rootRow, edge, edge},
		"root-then-edge": {rootRow, {"commit_hash": root, "parent_hash": child}},
		"edge-then-root": {{"commit_hash": root, "parent_hash": child}, rootRow},
		"invalid-commit": {{"commit_hash": "", "parent_hash": nil}},
		"missing-parent": {{"commit_hash": root}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sourceParentCensus(rows)
			var refused ErrSourceHistoryUnavailable
			if !errors.As(err, &refused) || refused.Reason != "invalid-ancestry" {
				t.Fatalf("invalid ancestry became a root: %v", err)
			}
		})
	}
}
