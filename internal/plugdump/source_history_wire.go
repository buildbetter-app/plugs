package plugdump

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const SourceHistoryFormat = "beads-dolt-images/1"
const SourceHistoryMember = "history/source.json"

var historyKinds = []string{"bead", "edge", "comment", "memory", "config"}

// SourceCommit retains Dolt attribution as opaque provenance, never a Plug
// principal. Commits are oldest first. A verified range admits only a complete
// linear chain; merges and missing parents remain archived, unverified evidence.
type SourceCommit struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Metadata Row      `json:"metadata"`
}

type HistoryGap struct {
	First  uint64 `json:"first"`
	Last   uint64 `json:"last"`
	Reason string `json:"reason"`
}

// SourceHistoryFrame is a complete portable business image read at CommitID,
// including labels/relations/comments/config. Events are the complete source
// audit table at that commit, not an inferred grouping of individual SQL writes.
type SourceHistoryFrame struct {
	Sequence uint64   `json:"sequence"`
	CommitID string   `json:"commit_id"`
	Records  []Record `json:"records"`
	Events   []Row    `json:"events"`
}

type SourceHistoryRange struct {
	Status         string               `json:"status"`
	Reason         string               `json:"reason"`
	Complete       bool                 `json:"complete"`
	First          uint64               `json:"first"`
	Head           uint64               `json:"head"`
	Baseline       []Record             `json:"baseline"`
	BaselineEvents []Row                `json:"baseline_events"`
	Gaps           []HistoryGap         `json:"gaps"`
	Frames         []SourceHistoryFrame `json:"frames"`
}

type SourceHistory struct {
	Format  string             `json:"format"`
	Commits []SourceCommit     `json:"commits"`
	Events  []Row              `json:"events"`
	Range   SourceHistoryRange `json:"range"`
}

// ErrSourceHistory distinguishes an invalid replay proof from an unavailable
// capture. Trusted restore hosts map Reason to their typed history refusal.
type ErrSourceHistory struct{ Reason string }

func (e ErrSourceHistory) Error() string { return "invalid source history: " + e.Reason }

func validateSourceHistory(manifest Manifest, members map[string][]byte) error {
	raw, present := members[SourceHistoryMember]
	if !present {
		if manifest.History == "verified" {
			return ErrSourceHistory{Reason: "available-unverified"}
		}
		return nil
	}
	var h SourceHistory
	if err := decodeJSON(raw, &h); err != nil {
		return fmt.Errorf("source history shape: %w", err)
	}
	fail := func(reason string) error { return ErrSourceHistory{Reason: reason} }
	if h.Format != SourceHistoryFormat || len(h.Commits) > 100000 || len(h.Range.Frames) > 100000 {
		return fail("unmappable")
	}
	for _, event := range h.Events {
		if err := validateSecretReferences(map[string]any(event)); err != nil {
			return err
		}
	}
	byKind := map[string][]Record{}
	for _, kind := range manifest.Kinds {
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			var r Record
			if err := decodeJSON(line, &r); err != nil {
				return err
			}
			byKind[kind.Name] = append(byKind[kind.Name], r)
		}
	}
	currentEvents := []Row{}
	for _, r := range byKind["beads-archive"] {
		if r.Source.Table == "events" {
			currentEvents = append(currentEvents, r.Source.Fields)
		}
	}
	if !sameEventImage(currentEvents, h.Events) {
		return fail("unmappable")
	}
	seenCommits := map[string]bool{}
	for _, c := range h.Commits {
		if !validCommitID(c.ID) || seenCommits[c.ID] || c.Metadata == nil {
			return fail("unmappable")
		}
		seenCommits[c.ID] = true
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return err
		}
		for _, p := range c.Parents {
			if !validCommitID(p) {
				return fail("unmappable")
			}
		}
	}
	r := h.Range
	if r.Status == "unverified" {
		if strings.TrimSpace(r.Reason) == "" || r.Complete || r.First != 0 || r.Head != 0 || len(r.Baseline)+len(r.BaselineEvents)+len(r.Frames)+len(r.Gaps) != 0 || manifest.History != "available-unverified" {
			return fail("available-unverified")
		}
		return nil
	}
	if r.Status != "verified" || r.Reason != "" || r.First == 0 || r.Head != uint64(len(h.Commits)) || r.Head < r.First || r.Head-r.First+1 != uint64(len(r.Frames)) {
		return fail("non-contiguous")
	}
	for i, c := range h.Commits {
		if i == 0 && len(c.Parents) != 0 || i > 0 && (len(c.Parents) != 1 || c.Parents[0] != h.Commits[i-1].ID) {
			return fail("non-contiguous")
		}
	}
	if r.First == 1 {
		if !r.Complete || len(r.Baseline)+len(r.BaselineEvents)+len(r.Gaps) != 0 || manifest.History != "verified" {
			return fail("non-contiguous")
		}
	} else {
		if r.Complete || manifest.History != "available-unverified" || validateHistoryImage(r.Baseline, manifest) != nil {
			return fail("unmappable")
		}
		next := uint64(1)
		for _, gap := range r.Gaps {
			if gap.First != next || gap.Last < gap.First || gap.Last >= r.First || strings.TrimSpace(gap.Reason) == "" {
				return fail("non-contiguous")
			}
			next = gap.Last + 1
		}
		if next != r.First {
			return fail("non-contiguous")
		}
	}
	previous := r.BaselineEvents
	for i, frame := range r.Frames {
		if frame.Sequence != r.First+uint64(i) || frame.CommitID != h.Commits[frame.Sequence-1].ID {
			return fail("non-contiguous")
		}
		if validateHistoryImage(frame.Records, manifest) != nil || !extendsEvents(previous, frame.Events) {
			return fail("unmappable")
		}
		previous = frame.Events
	}
	current := []Record{}
	for _, kind := range historyKinds {
		current = append(current, byKind[kind]...)
	}
	if !sameRecordImage(current, r.Frames[len(r.Frames)-1].Records) || !sameEventImage(h.Events, previous) {
		return fail("unmappable")
	}
	return nil
}

func validCommitID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'v') {
			return false
		}
	}
	return true
}

func validateHistoryImage(rows []Record, manifest Manifest) error {
	seen, beads := map[string]bool{}, map[string]string{}
	counts := map[string]int{}
	issues := []Record{}
	for _, r := range rows {
		if r.Kind == "beads-archive" || !knownKinds[r.Kind] || seen[historyRecordKey(r)] {
			return fmt.Errorf("duplicate or nonportable history record")
		}
		seen[historyRecordKey(r)] = true
		if err := validateRecord(r, manifest.Projects, counts); err != nil {
			return err
		}
		if r.Kind == "bead" {
			if beads[r.ID] != "" {
				return fmt.Errorf("duplicate historical bead")
			}
			beads[r.ID] = r.Project
			issues = append(issues, r)
		}
	}
	for _, r := range rows {
		if r.Kind == "edge" || r.Kind == "comment" {
			if beads[historyText(r.Source.Fields["issue_id"])] != r.Project || r.Kind == "edge" && r.Body["target_kind"] == "depends_on_issue_id" && beads[historyText(r.Body["to"])] == "" {
				return fmt.Errorf("dangling historical relation")
			}
		}
	}
	return validateOntologyRecords(applicableHistoryOntology(manifest.Ontology, issues), manifest.Projects, issues)
}

// Store identity is supplied by the enclosing manifest/history source tuple.
// Kind/table/project/raw ID are compared without relabeling physical IDs.
func historyRecordKey(r Record) string {
	key, _ := json.Marshal([4]string{r.Project, r.Kind, r.Source.Table, r.ID})
	return string(key)
}

func sameRecordImage(a, b []Record) bool {
	canonical := func(rows []Record) []Record {
		copy := append([]Record{}, rows...)
		sort.Slice(copy, func(i, j int) bool { return historyRecordKey(copy[i]) < historyRecordKey(copy[j]) })
		return copy
	}
	return equalJSON(canonical(a), canonical(b))
}

// SourceEventID retains source IDs exactly. Numeric SQL identifiers are
// lossless decimal strings; an event is not a verified principal or revision.
func SourceEventID(r Row) string {
	switch id := r["id"].(type) {
	case string:
		return id
	case json.Number:
		return string(id)
	case int64:
		return strconv.FormatInt(id, 10)
	case uint64:
		return strconv.FormatUint(id, 10)
	case int:
		return strconv.Itoa(id)
	default:
		return ""
	}
}

func eventImage(rows []Row) (map[string]Row, bool) {
	result := map[string]Row{}
	for _, row := range rows {
		id := SourceEventID(row)
		if id == "" || result[id] != nil || len(row) != len(strings.Fields(tableColumns["events"])) {
			return nil, false
		}
		for _, name := range strings.Fields(tableColumns["events"]) {
			if _, ok := row[name]; !ok {
				return nil, false
			}
		}
		if validateSecretReferences(map[string]any(row)) != nil {
			return nil, false
		}
		result[id] = row
	}
	return result, true
}

func sameEventImage(a, b []Row) bool {
	left, ok := eventImage(a)
	right, otherOK := eventImage(b)
	return ok && otherOK && equalJSON(left, right)
}

func extendsEvents(before, after []Row) bool {
	left, ok := eventImage(before)
	right, otherOK := eventImage(after)
	if !ok || !otherOK {
		return false
	}
	for id, row := range left {
		if !equalJSON(row, right[id]) {
			return false
		}
	}
	return true
}

func historyText(v any) string { s, _ := v.(string); return s }
func equalJSON(a, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}
