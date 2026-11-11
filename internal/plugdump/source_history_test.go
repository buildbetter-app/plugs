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

func TestSourcePlaneHistoryUsesMappedImageAllocatorCounters(t *testing.T) {
	for _, mode := range []string{"sequential", "hash", "namespaced-hash"} {
		t.Run(mode, func(t *testing.T) {
			s, inventory := historyFixture(t)
			inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
			inventory.Projects[0].LastID = 0
			inventory.Projects[0].ChildCounters = map[string]uint64{}
			parent := "demo-7"
			if mode != "sequential" {
				inventory.Projects[0].IDMode = "hash"
				inventory.Projects[0].HashLength = 1
			}
			if mode == "namespaced-hash" {
				inventory.Projects[0].HashNamespaces = []string{"abc"}
				parent = "demo-abc-a7b"
				replace := strings.NewReplacer(`"demo-7"`, `"demo-abc-a7b"`, `"demo-8"`, `"demo-abc-c8d"`, `"demo-7.4"`, `"demo-abc-a7b.4"`)
				remap := func(image fixtureSource) fixtureSource {
					var mapped fixtureSource
					if err := decodeJSON([]byte(replace.Replace(string(mustJSON(t, image)))), &mapped); err != nil {
						t.Fatal(err)
					}
					return mapped
				}
				s.fixtureSource = remap(s.fixtureSource)
				for id, image := range s.images {
					s.images[id] = remap(image)
				}
			}
			// The older image has its own smaller observed child counter. No
			// source field or allocator value may be filled from the head.
			first := s.images[s.log.Commits[0].ID]
			first["issues"].Rows[2]["id"] = parent + ".2"
			first["dependencies"].Rows[0]["issue_id"] = parent + ".2"
			first["child_counters"].Rows[0]["last_child"] = 1
			original := mustJSON(t, inventory)
			data := buildFixture(t, s, inventory)
			manifest, h := sourceCapture(t, data)
			if manifest.History != "verified" || h.Range.Status != "verified" || !h.Range.Complete || len(h.Range.Frames) != 4 {
				t.Fatalf("mapped allocator image lost: status=%s reason=%s frames=%d", h.Range.Status, h.Range.Reason, len(h.Range.Frames))
			}
			if manifest.Projects[0].ChildCounters[parent] != 4 || !bytes.Equal(original, mustJSON(t, inventory)) {
				t.Fatal("mapped source counters were lost or input inventory was mutated")
			}
			seen := map[string]bool{}
			for _, record := range h.Range.Frames[0].Records {
				if record.Kind == "bead" {
					seen[record.ID] = true
				}
			}
			if !seen[parent+".2"] || seen[parent+".4"] {
				t.Fatal("older child identity borrowed current source state")
			}
			// The reader must still reject a history whose actual child ID
			// exceeds the final manifest's explicitly bound allocator counter.
			manifest.Projects[0].ChildCounters[parent] = 3
			var refusal ErrSourceHistory
			if err := validateSourceHistory(manifest, archiveMembers(t, data)); !errors.As(err, &refusal) || refusal.Reason != "unmappable" {
				t.Fatalf("history validation lost the child-counter refusal: %v", err)
			}
		})
	}
}

func TestExportSourceHistoryProvesEmptyRoot(t *testing.T) {
	s, inventory := historyFixture(t)
	root := s.log.Commits[0].ID
	// The reader has successfully enumerated every table at the actual root.
	// No schema, records, or events existed yet; this is not a failed read.
	s.images[root] = map[string]Table{}
	m, h := sourceCapture(t, buildFixture(t, s, inventory))
	if m.History != "verified" || !h.Range.Complete || h.Range.First != 1 || h.Range.Head != 4 || len(h.Range.Frames) != 4 || len(h.Range.Gaps) != 0 {
		t.Fatalf("observed empty root was not proven: %+v", h.Range)
	}
	frame := h.Range.Frames[0]
	if frame.CommitID != root || frame.Sequence != 1 || frame.Records == nil || frame.Events == nil || len(frame.Records)+len(frame.Events) != 0 {
		t.Fatalf("empty root acquired invented source state: %+v", frame)
	}
}

func TestExportSourceHistoryEmptyRootDoesNotFillMissingImages(t *testing.T) {
	for _, mode := range []string{"nil-root", "non-root", "incomplete-schema", "unknown-table"} {
		t.Run(mode, func(t *testing.T) {
			s, inventory := historyFixture(t)
			root := s.log.Commits[0].ID
			switch mode {
			case "nil-root":
				s.images[root] = nil
			case "non-root":
				s.images[s.log.Commits[1].ID] = map[string]Table{}
			case "incomplete-schema":
				s.images[root] = map[string]Table{"issues": {Columns: columnsFor("issues"), Rows: []Row{}}}
			case "unknown-table":
				s.images[root] = map[string]Table{"unknown": {Columns: []string{"id"}, Rows: []Row{}}}
			}
			m, h := sourceCapture(t, buildFixture(t, s, inventory))
			if m.History != "available-unverified" || h.Range.Complete || len(h.Range.Gaps) == 0 || h.Range.First == 1 {
				t.Fatalf("missing or unsupported source image became complete: %+v", h.Range)
			}
		})
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

func TestExportSourceHistoryDeterministicEventPermutations(t *testing.T) {
	for _, mode := range []string{"full", "partial", "unverified"} {
		permutations := []string{"current", "baseline", "frame", "head-frame", "all"}
		if mode == "unverified" {
			permutations = []string{"current", "all"}
		}
		for _, permutation := range permutations {
			t.Run(mode+"/"+permutation, func(t *testing.T) {
				s, inventory := historyFixture(t)
				template := s.fixtureSource["events"].Rows[0]
				// Consecutive IDs above 2^53 must stay distinct. An opaque
				// string ID and stored values must also survive unchanged.
				ids := []any{json.Number("9007199254740993"), json.Number("9007199254740992"), "event/opaque"}
				setEvents := func(tables map[string]Table, count int) {
					table := tables["events"]
					table.Rows = []Row{}
					for i, id := range ids[:count] {
						row := cloneRow(template)
						row["id"] = id
						row["comment"] = "opaque\x00event " + fmt.Sprint(i)
						row["new_value"] = map[string]any{"n": json.Number("9007199254740993"), "null": nil}
						table.Rows = append(table.Rows, row)
					}
					tables["events"] = table
				}
				setEvents(s.fixtureSource, 3)
				for _, commit := range s.log.Commits {
					count := 2
					if commit.ID == s.log.Head {
						count = 3
					}
					setEvents(s.images[commit.ID], count)
				}
				if mode == "partial" {
					delete(s.images, s.log.Commits[0].ID)
				} else if mode == "unverified" {
					s.log.Clean = false
				}
				buildUnchanged := func() []byte {
					t.Helper()
					input := []any{s.fixtureSource, s.images, s.log}
					before, err := marshal(input)
					if err != nil {
						t.Fatal(err)
					}
					data := buildFixture(t, s, inventory)
					after, err := marshal(input)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("export changed caller-owned source rows, values or commit census")
					}
					return data
				}
				first := buildUnchanged()
				_, h := sourceCapture(t, first)
				if len(h.Events) != 3 || !sameEventImage(h.Events, s.fixtureSource["events"].Rows) {
					t.Fatal("current event identities or stored values changed")
				}
				switch mode {
				case "full":
					if !h.Range.Complete || len(h.Range.Frames) != 4 || len(h.Range.Frames[0].Events) != 2 {
						t.Fatal("fixture did not prove full history with historical events")
					}
				case "partial":
					if h.Range.Status != "verified" || h.Range.First != 3 || len(h.Range.BaselineEvents) != 2 || len(h.Range.Frames) != 2 {
						t.Fatal("fixture did not prove a partial baseline and retained frames")
					}
				case "unverified":
					if h.Range.Status != "unverified" || h.Range.Reason != "working-set-not-clean" {
						t.Fatal("fixture did not retain raw unverified events")
					}
				}
				reverse := func(tables map[string]Table) {
					rows := tables["events"].Rows
					for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
						rows[i], rows[j] = rows[j], rows[i]
					}
				}
				if permutation == "current" || permutation == "all" {
					reverse(s.fixtureSource)
				}
				for i, commit := range s.log.Commits {
					if permutation == "all" || permutation == "baseline" && i == 1 || permutation == "frame" && i == 2 || permutation == "head-frame" && commit.ID == s.log.Head {
						reverse(s.images[commit.ID])
					}
				}
				second := buildUnchanged()
				if !bytes.Equal(archiveMembers(t, first)[SourceHistoryMember], archiveMembers(t, second)[SourceHistoryMember]) {
					t.Error("equivalent event-row permutations changed history/source.json")
				}
				if !bytes.Equal(first, second) {
					t.Error("equivalent event-row permutations changed archive bytes")
				}
			})
		}
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

func TestExportSourceHistoryNamesMissingHeadFamiliesWithoutBackfill(t *testing.T) {
	for _, test := range []struct {
		name    string
		missing []string
		reason  string
	}{
		{"audit", []string{"events"}, "head-image-missing-families:events"},
		{"native-planes", []string{"wisps", "ignored_schema_migrations", "events"}, "head-image-missing-families:events,ignored_schema_migrations,wisps"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, inventory := historyFixture(t)
			image := s.images[s.log.Head]
			for _, name := range test.missing {
				delete(image, name)
			}
			beforeImage, beforeHead := mustJSON(t, image), mustJSON(t, s.fixtureSource)
			m, h := sourceCapture(t, buildFixture(t, s, inventory))
			if m.History != "available-unverified" || h.Range.Status != "unverified" || h.Range.Reason != test.reason || h.Range.Complete || h.Range.First != 0 || h.Range.Head != 0 || len(h.Range.Frames)+len(h.Range.Baseline)+len(h.Range.BaselineEvents)+len(h.Range.Gaps) != 0 {
				t.Fatalf("missing historical families were not explicitly refused: %+v", h.Range)
			}
			if len(h.Commits) != 4 || !sameEventImage(h.Events, s.fixtureSource["events"].Rows) {
				t.Fatal("gap reporting discarded actual commit or working-set audit evidence")
			}
			if !bytes.Equal(beforeImage, mustJSON(t, image)) || !bytes.Equal(beforeHead, mustJSON(t, s.fixtureSource)) {
				t.Fatal("missing historical family was backfilled or source head mutated")
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

func TestStatusCategoryHistoryPreservesNamesAndRefusesChangedCatalog(t *testing.T) {
	for _, mode := range []string{"YAML only", "SQL and YAML agree", "older SQL declaration absent"} {
		t.Run(mode, func(t *testing.T) {
			source, inventory := historyFixture(t)
			inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
			inventory.StatusCategoryContract = StatusCategoryContract
			inventory.Projects[0].Statuses = append(nativeBuiltInStatuses(), ProjectStatus{"todo", "active"})
			setCatalogYAML(t, &inventory, []byte("status:\n  custom: [todo:active]\n"))
			images := []map[string]Table{source.fixtureSource}
			for _, image := range source.images {
				images = append(images, image)
			}
			for _, image := range images {
				image["issues"].Rows[0]["status"] = "todo"
				rows := []Row{}
				if mode != "YAML only" {
					rows = append(rows, Row{"name": "todo", "category": "active"})
				}
				image["custom_statuses"] = Table{Columns: columnsFor("custom_statuses"), Rows: rows}
			}
			if mode == "older SQL declaration absent" {
				image := source.images[source.log.Commits[0].ID]
				image["custom_statuses"] = Table{Columns: columnsFor("custom_statuses")}
			}
			_, history := sourceCapture(t, buildFixture(t, source, inventory))
			switch mode {
			case "YAML only":
				if history.Range.Status != "unverified" || history.Range.Reason != "head-image-not-proven" || len(history.Range.Frames) != 0 {
					t.Fatal("head-only YAML invented historical status declarations")
				}
			case "older SQL declaration absent":
				if history.Range.Status != "verified" || history.Range.Complete || len(history.Range.Gaps) == 0 {
					t.Fatal("head YAML filled an older missing SQL catalog")
				}
			case "SQL and YAML agree":
				if history.Range.Status != "verified" || !history.Range.Complete {
					t.Fatalf("actual SQL declaration became unavailable: %+v", history.Range)
				}
			}
		})
	}
	s, inventory := historyFixture(t)
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	inventory.StatusCategoryContract = StatusCategoryContract
	inventory.Projects[0].Statuses = append(nativeBuiltInStatuses(), ProjectStatus{"todo", "active"})
	apply := func(image map[string]Table) {
		image["custom_statuses"] = Table{Columns: columnsFor("custom_statuses"), Rows: []Row{{"name": "todo", "category": "active"}}}
		image["issues"].Rows[0]["status"] = "todo"
	}
	apply(s.fixtureSource)
	for _, image := range s.images {
		apply(image)
	}
	_, h := sourceCapture(t, buildFixture(t, s, inventory))
	if h.Range.Status != "verified" || !h.Range.Complete {
		t.Fatalf("status history not proven: %+v", h.Range)
	}
	for _, frame := range h.Range.Frames {
		for _, record := range frame.Records {
			if record.ID == "demo-7" && (record.Source.Fields["status"] != "todo" || record.Body["status_category"] != "active") {
				t.Fatal("historical status normalized")
			}
		}
	}
	// The portable wire has no per-frame project catalog. A genuinely different
	// older declaration must remain an explicit gap, never borrow today's one.
	s.images[s.log.Commits[0].ID]["custom_statuses"].Rows[0]["category"] = "wip"
	_, h = sourceCapture(t, buildFixture(t, s, inventory))
	if h.Range.Status != "verified" || h.Range.Complete || len(h.Range.Gaps) == 0 {
		t.Fatal("changed older category borrowed current semantics")
	}
}
