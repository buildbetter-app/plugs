package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
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

func (a *Archive) SourceHistory() (SourceHistory, bool) {
	if a == nil || a.history == nil {
		return SourceHistory{}, false
	}
	var h SourceHistory
	// The member was shape-checked by Read. Decode again to detach every nested
	// row while preserving JSON numbers and nulls.
	_ = decodeJSON(a.members[SourceHistoryMember], &h)
	return h, true
}

// HistoryImage exposes only an image already checked by Read. Sequence First-1
// selects the proven partial baseline; no arbitrary records can be projected.
func (a *Archive) HistoryImage(sequence uint64) (*Archive, error) {
	h, ok := a.SourceHistory()
	if !ok || h.Range.Status != "verified" {
		return nil, ErrSourceHistory{Reason: "available-unverified"}
	}
	var rows []Record
	if h.Range.First > 1 && sequence == h.Range.First-1 {
		rows = h.Range.Baseline
	} else if sequence >= h.Range.First && sequence <= h.Range.Head {
		rows = h.Range.Frames[sequence-h.Range.First].Records
	} else {
		return nil, ErrSourceHistory{Reason: "non-contiguous"}
	}
	image := &Archive{sha256: a.sha256, manifest: a.Manifest(), members: a.members, byKind: map[string][]Record{}}
	for _, r := range rows {
		image.byKind[r.Kind] = append(image.byKind[r.Kind], r)
	}
	// Read-only census/provenance is retained at restore time; it is never used
	// to fill missing historical business fields.
	image.byKind["beads-archive"] = a.Records("beads-archive")
	return image, nil
}

func validateSourceHistory(a *Archive) error {
	raw, present := a.members[SourceHistoryMember]
	if !present {
		if a.manifest.History == "verified" {
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
	currentEvents := []Row{}
	for _, r := range a.byKind["beads-archive"] {
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
		if strings.TrimSpace(r.Reason) == "" || r.Complete || r.First != 0 || r.Head != 0 || len(r.Baseline)+len(r.BaselineEvents)+len(r.Frames)+len(r.Gaps) != 0 || a.manifest.History != "available-unverified" {
			return fail("available-unverified")
		}
		a.history = &h
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
		if !r.Complete || len(r.Baseline)+len(r.BaselineEvents)+len(r.Gaps) != 0 || a.manifest.History != "verified" {
			return fail("non-contiguous")
		}
	} else {
		if r.Complete || a.manifest.History != "available-unverified" || validateHistoryImage(r.Baseline, a.manifest) != nil {
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
		if validateHistoryImage(frame.Records, a.manifest) != nil || !extendsEvents(previous, frame.Events) {
			return fail("unmappable")
		}
		previous = frame.Events
	}
	current := []Record{}
	for _, kind := range historyKinds {
		current = append(current, a.byKind[kind]...)
	}
	if !sameRecordImage(current, r.Frames[len(r.Frames)-1].Records) || !sameEventImage(h.Events, previous) {
		return fail("unmappable")
	}
	a.history = &h
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
	beadRows := []Record{}
	for _, r := range rows {
		if r.Kind == "beads-archive" || !knownKinds[r.Kind] || seen[recordKey(r)] {
			return fmt.Errorf("duplicate or nonportable history record")
		}
		seen[recordKey(r)] = true
		if err := validateVersionedRecord(r, manifest.Projects, counts, nil, manifest.EdgeVersion()); err != nil {
			return err
		}
		if r.Kind == "bead" {
			if beads[r.ID] != "" {
				return fmt.Errorf("duplicate historical bead")
			}
			beads[r.ID] = r.Project
			beadRows = append(beadRows, r)
		}
	}
	for _, r := range rows {
		if r.Kind == "edge" || r.Kind == "comment" {
			if beads[stringValue(r.Source.Fields["issue_id"])] != r.Project || r.Kind == "edge" && r.Body["target_kind"] == "depends_on_issue_id" && beads[stringValue(r.Body["to"])] == "" {
				return fmt.Errorf("dangling historical relation")
			}
		}
	}
	return validateIssueOntology(manifest.Ontology, beadRows, false)
}

func sameRecordImage(a, b []Record) bool {
	canonical := func(rows []Record) []Record {
		copy := append([]Record{}, rows...)
		sort.Slice(copy, func(i, j int) bool { return recordKey(copy[i]) < recordKey(copy[j]) })
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
