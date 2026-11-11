package plugdump

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const NativeHistoryCoverageFormat = "beads-native-history-coverage/1"

type NativeHistoryBoundary struct {
	CommitID              string   `json:"commit_id"`
	ChildCommitIDs        []string `json:"child_commit_ids"`
	CommitOrdinalFromHead uint64   `json:"commit_ordinal_from_head"`
	CommitSchemaVersion   *uint64  `json:"commit_schema_version,omitempty"`
	Reason                string   `json:"reason"`
	SQLDoltCodes          []uint16 `json:"sql_dolt_codes,omitempty"`
}

type NativeEventsAttachment struct {
	Member string `json:"member"`
	SHA256 string `json:"sha256"`
	Rows   uint64 `json:"rows"`
}

// Coverage authenticates a verified reachable prefix, not complete source
// history. Boundary nodes are external parent edges, never fabricated commits.
type NativeHistoryCoverage struct {
	Format              string                  `json:"format"`
	Status              string                  `json:"status"`
	VerifiedCommitCount uint64                  `json:"verified_commit_count"`
	Boundaries          []NativeHistoryBoundary `json:"boundaries"`
	EventsAttachment    NativeEventsAttachment  `json:"events_attachment"`
}

func validHistoryBoundaryReason(reason string) bool {
	switch reason {
	case "ghost_parent", "missing_root_chunk", "missing_table_chunk", "missing_archive_chunk", "unreadable_schema", "other":
		return true
	}
	return false
}

// historyCommitOrder keeps the legacy all-parent verifier unchanged. Only an
// explicitly declared frontier permits an external parent, with exact edges and
// extended canonical ordinals checked by the separate boundary verifier.
func historyCommitOrder(log SourceCommitLog) ([]SourceCommit, error) {
	if len(log.Boundaries) == 0 {
		return nativeCommitOrder(log)
	}
	ordered, _, err := boundaryCommitOrder(log, true)
	return ordered, err
}

func historyCommitOrdinal(log SourceCommitLog, id string) uint64 {
	if len(log.Boundaries) > 0 {
		_, ordinals, err := boundaryCommitOrder(log, true)
		if err == nil {
			return ordinals[id]
		}
		return 0
	}
	ordered, err := nativeCommitOrder(log)
	if err != nil {
		return 0
	}
	for i, c := range ordered {
		if c.ID == id {
			return uint64(len(ordered) - i)
		}
	}
	return 0
}

// NativeCommitOrdinal returns a verified graph coordinate, including frontier
// terminal nodes in the canonical order. Zero means the coordinate is unproved.
func NativeCommitOrdinal(h NativeHistory, id string) uint64 {
	log := SourceCommitLog{Head: h.Head, Commits: h.Commits}
	if h.HistoryCoverage != nil {
		log.Boundaries = h.HistoryCoverage.Boundaries
	}
	return historyCommitOrdinal(log, id)
}

func boundaryCommitOrder(log SourceCommitLog, checkCoordinates bool) ([]SourceCommit, map[string]uint64, error) {
	invalid := func() ([]SourceCommit, map[string]uint64, error) {
		return nil, nil, ErrSourceHistoryUnavailable{Reason: "invalid-native-history-boundary"}
	}
	if len(log.Commits) == 0 || len(log.Commits) > 10000 || len(log.Boundaries) == 0 || len(log.Boundaries) > 10000 || !validCommitID(log.Head) {
		return invalid()
	}
	commits := map[string]SourceCommit{}
	for _, c := range log.Commits {
		if !validCommitID(c.ID) || c.Metadata == nil {
			return invalid()
		}
		if _, exists := commits[c.ID]; exists {
			return invalid()
		}
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return nil, nil, err
		}
		commits[c.ID] = c
	}
	if _, present := commits[log.Head]; !present {
		return invalid()
	}
	frontier := map[string]NativeHistoryBoundary{}
	for i, b := range log.Boundaries {
		if !validCommitID(b.CommitID) || !validHistoryBoundaryReason(b.Reason) || i > 0 && log.Boundaries[i-1].CommitID >= b.CommitID {
			return invalid()
		}
		if _, present := commits[b.CommitID]; present {
			return invalid()
		}
		for j, code := range b.SQLDoltCodes {
			if code == 0 || j > 0 && b.SQLDoltCodes[j-1] >= code {
				return invalid()
			}
		}
		if b.Reason == "ghost_parent" && b.CommitSchemaVersion != nil {
			return invalid()
		}
		frontier[b.CommitID] = b
	}
	children := map[string][]string{}
	for _, c := range log.Commits {
		parents := append([]string{}, c.Parents...)
		sort.Strings(parents)
		for i, p := range parents {
			if !validCommitID(p) || i > 0 && parents[i-1] == p {
				return invalid()
			}
			if _, present := commits[p]; !present {
				if _, boundary := frontier[p]; !boundary {
					return invalid()
				}
				children[p] = append(children[p], c.ID)
			}
		}
	}
	for _, b := range log.Boundaries {
		sort.Strings(children[b.CommitID])
		if len(children[b.CommitID]) == 0 || checkCoordinates && !equalJSON(children[b.CommitID], b.ChildCommitIDs) {
			return invalid()
		}
	}
	state := map[string]int{}
	extended := []string{}
	ordered := []SourceCommit{}
	var visit func(string) bool
	visit = func(id string) bool {
		if state[id] == 1 {
			return false
		}
		if state[id] == 2 {
			return true
		}
		state[id] = 1
		if c, present := commits[id]; present {
			parents := append([]string{}, c.Parents...)
			sort.Strings(parents)
			for _, p := range parents {
				if !visit(p) {
					return false
				}
			}
			ordered = append(ordered, c)
		} else if _, present := frontier[id]; !present {
			return false
		}
		state[id] = 2
		extended = append(extended, id)
		return true
	}
	if !visit(log.Head) || len(ordered) != len(commits) || len(extended) != len(commits)+len(frontier) {
		return invalid()
	}
	ordinals := map[string]uint64{}
	for i, id := range extended {
		ordinals[id] = uint64(len(extended) - i)
	}
	for _, b := range log.Boundaries {
		if checkCoordinates && b.CommitOrdinalFromHead != ordinals[b.CommitID] {
			return invalid()
		}
	}
	return ordered, ordinals, nil
}

// locateHistoryBoundaries is used only after the physical reader has supplied
// positive frontier evidence. Validation never repairs an incoming descriptor.
func locateHistoryBoundaries(log *SourceCommitLog) error {
	sort.Slice(log.Boundaries, func(i, j int) bool { return log.Boundaries[i].CommitID < log.Boundaries[j].CommitID })
	_, ordinals, err := boundaryCommitOrder(*log, false)
	if err != nil {
		return err
	}
	for i := range log.Boundaries {
		b := &log.Boundaries[i]
		b.CommitOrdinalFromHead = ordinals[b.CommitID]
		b.ChildCommitIDs = []string{}
		for _, c := range log.Commits {
			for _, p := range c.Parents {
				if p == b.CommitID {
					b.ChildCommitIDs = append(b.ChildCommitIDs, c.ID)
				}
			}
		}
		sort.Strings(b.ChildCommitIDs)
	}
	return nil
}

func validateHistoryCoverage(h NativeHistory, source []byte, portable SourceHistory) error {
	c := h.HistoryCoverage
	if c == nil {
		return nil
	}
	if c.Format != NativeHistoryCoverageFormat || c.Status != "verified-to-boundary" || c.VerifiedCommitCount == 0 || len(c.Boundaries) == 0 || c.VerifiedCommitCount != uint64(len(h.Commits)) || len(h.Images) != len(h.Commits) || h.Complete || len(h.Gaps) != 0 {
		return fmt.Errorf("invalid native history coverage")
	}
	if _, err := historyCommitOrder(SourceCommitLog{Head: h.Head, Commits: h.Commits, Boundaries: c.Boundaries}); err != nil {
		return err
	}
	a := c.EventsAttachment
	if a.Member != SourceHistoryMember || a.SHA256 != fmt.Sprintf("%x", sha256.Sum256(source)) || a.Rows != uint64(len(portable.Events)) || a.Rows != uint64(len(h.Current["events"].Rows)) || !sameEventImage(portable.Events, h.Current["events"].Rows) {
		return fmt.Errorf("native history events attachment differs")
	}
	r := portable.Range
	if r.Status != "unverified" || r.Reason != "native-history-boundary" || r.Complete || r.First != 0 || r.Head != 0 || len(r.Frames)+len(r.Gaps)+len(r.Baseline)+len(r.BaselineEvents) != 0 {
		return fmt.Errorf("boundary history cannot claim portable completeness")
	}
	return nil
}

// Presence is distinct from JSON null/omission, including a measured zero
// events count. Keep the new descriptor's exact lower-case wire spelling.
func validateHistoryCoverageShape(raw []byte) error {
	var native map[string]json.RawMessage
	if err := json.Unmarshal(raw, &native); err != nil {
		return err
	}
	data, present := native["history_coverage"]
	for key := range native {
		if key != "history_coverage" && strings.EqualFold(key, "history_coverage") {
			return fmt.Errorf("noncanonical history coverage field")
		}
	}
	if !present {
		return nil
	}
	object := func(data []byte, required, optional []string) (map[string]json.RawMessage, error) {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
			return nil, fmt.Errorf("history coverage object required")
		}
		allowed := map[string]bool{}
		for _, key := range append(append([]string{}, required...), optional...) {
			allowed[key] = true
		}
		for key, v := range fields {
			if !allowed[key] || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return nil, fmt.Errorf("invalid history coverage field")
			}
		}
		for _, key := range required {
			if _, present := fields[key]; !present {
				return nil, fmt.Errorf("missing history coverage field")
			}
		}
		return fields, nil
	}
	fields, err := object(data, []string{"format", "status", "verified_commit_count", "boundaries", "events_attachment"}, nil)
	if err != nil {
		return err
	}
	if _, err := object(fields["events_attachment"], []string{"member", "sha256", "rows"}, nil); err != nil {
		return err
	}
	var boundaries []json.RawMessage
	if err := json.Unmarshal(fields["boundaries"], &boundaries); err != nil || len(boundaries) == 0 {
		return fmt.Errorf("history frontier required")
	}
	for _, b := range boundaries {
		fields, err := object(b, []string{"commit_id", "child_commit_ids", "commit_ordinal_from_head", "reason"}, []string{"commit_schema_version", "sql_dolt_codes"})
		if err != nil {
			return err
		}
		if codes, present := fields["sql_dolt_codes"]; present {
			var values []uint16
			if err := json.Unmarshal(codes, &values); err != nil || len(values) == 0 {
				return fmt.Errorf("empty history SQL codes must be omitted")
			}
		}
	}
	return nil
}
