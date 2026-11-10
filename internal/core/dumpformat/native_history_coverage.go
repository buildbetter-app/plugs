package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const NativeHistoryCoverageFormat = "beads-native-history-coverage/1"

// NativeHistoryBoundary is a genuinely unavailable parent of the verified
// source graph. The ID and exact observed child edges are source provenance;
// safe projections may export only its ordinal, observed schema and codes.
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

// NativeHistoryCoverage describes only the verified graph and its real
// unavailable frontier. It never asserts how many commits precede a boundary.
type NativeHistoryCoverage struct {
	Format              string                  `json:"format"`
	Status              string                  `json:"status"`
	VerifiedCommitCount uint64                  `json:"verified_commit_count"`
	Boundaries          []NativeHistoryBoundary `json:"boundaries"`
	EventsAttachment    NativeEventsAttachment  `json:"events_attachment"`
}

func (c *NativeHistoryCoverage) UnmarshalJSON(raw []byte) error {
	if _, err := nativeObject(raw, "format status verified_commit_count boundaries events_attachment", ""); err != nil {
		return err
	}
	type plain NativeHistoryCoverage
	var value plain
	if err := decodeJSON(raw, &value); err != nil {
		return err
	}
	*c = NativeHistoryCoverage(value)
	return nil
}

func (a *NativeEventsAttachment) UnmarshalJSON(raw []byte) error {
	if _, err := nativeObject(raw, "member sha256 rows", ""); err != nil {
		return err
	}
	type plain NativeEventsAttachment
	var value plain
	if err := decodeJSON(raw, &value); err != nil {
		return err
	}
	*a = NativeEventsAttachment(value)
	return nil
}

func (b *NativeHistoryBoundary) UnmarshalJSON(raw []byte) error {
	object, err := nativeObject(raw, "commit_id child_commit_ids commit_ordinal_from_head reason", "commit_schema_version sql_dolt_codes")
	if err != nil {
		return err
	}
	if codes, present := object["sql_dolt_codes"]; present {
		if list, ok := codes.([]any); !ok || len(list) == 0 {
			return fmt.Errorf("noncanonical native boundary SQL/Dolt codes")
		}
	}
	type plain NativeHistoryBoundary
	var value plain
	if err := decodeJSON(raw, &value); err != nil {
		return err
	}
	*b = NativeHistoryBoundary(value)
	return nil
}

func nativeBoundaryReason(reason string) bool {
	switch reason {
	case "ghost_parent", "missing_root_chunk", "missing_table_chunk", "missing_archive_chunk", "unreadable_schema", "other":
		return true
	}
	return false
}

func nativeSourceAttachmentPath(project string) string {
	return "history/source-attachments/" + project + ".json"
}

func scopedNativeSourceAttachment(name string) (string, bool) {
	const prefix = "history/source-attachments/"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".json") {
		return "", false
	}
	project := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".json")
	return project, prefixPattern.MatchString(project)
}

func validateNativeCoverageShape(h NativeHistory) error {
	c := h.HistoryCoverage
	if c == nil {
		return nil
	}
	if c.Format != NativeHistoryCoverageFormat || c.Status != "verified-to-boundary" || h.Complete || len(h.Gaps) != 0 || len(h.Commits) == 0 || len(h.Images) != len(h.Commits) || c.VerifiedCommitCount != uint64(len(h.Commits)) || len(c.Boundaries) == 0 || len(c.Boundaries) > 10000 {
		return nativeGap(h, "invalid-native-history-coverage")
	}
	if c.EventsAttachment.Member != SourceHistoryMember || len(c.EventsAttachment.SHA256) != 64 {
		return nativeGap(h, "invalid-native-events-attachment")
	}
	for _, ch := range c.EventsAttachment.SHA256 {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return nativeGap(h, "invalid-native-events-attachment")
		}
	}
	for i, boundary := range c.Boundaries {
		if !validCommitID(boundary.CommitID) || i > 0 && boundary.CommitID <= c.Boundaries[i-1].CommitID || len(boundary.ChildCommitIDs) == 0 || len(boundary.ChildCommitIDs) > len(h.Commits) || !nativeBoundaryReason(boundary.Reason) || boundary.CommitOrdinalFromHead < 2 || boundary.CommitOrdinalFromHead > uint64(len(h.Commits)+len(c.Boundaries)) {
			return nativeGap(h, "invalid-native-history-boundary")
		}
		if boundary.Reason == "ghost_parent" && boundary.CommitSchemaVersion != nil {
			return nativeGap(h, "ghost-boundary-schema-unreadable")
		}
		for j, child := range boundary.ChildCommitIDs {
			if !validCommitID(child) || j > 0 && child <= boundary.ChildCommitIDs[j-1] {
				return nativeGap(h, "invalid-native-boundary-child-census")
			}
		}
		for j, code := range boundary.SQLDoltCodes {
			if code == 0 || j > 0 && code <= boundary.SQLDoltCodes[j-1] {
				return nativeGap(h, "invalid-native-boundary-sql-codes")
			}
		}
	}
	return nil
}

// RequireVerifiedNativeHistory admits a complete root history or a verified
// graph whose only unavailable parents are authenticated boundary descriptors.
// Arbitrary named gaps retain their original strict receiving refusal.
func RequireVerifiedNativeHistory(h NativeHistory) error {
	if err := validateNativeCoverageShape(h); err != nil {
		return err
	}
	if h.HistoryCoverage != nil {
		return nativeCommitCensus(h)
	}
	return requireCompleteNativeHistory(h)
}

func nativeAttachmentBytes(a *Archive, h NativeHistory, scoped bool) ([]byte, error) {
	path := SourceHistoryMember
	if scoped {
		if len(h.Projects) != 1 {
			return nil, nativeGap(h, "ambiguous-native-events-attachment")
		}
		path = nativeSourceAttachmentPath(h.Projects[0])
	}
	raw, present := a.members[path]
	if !present || h.HistoryCoverage == nil || digest(raw) != h.HistoryCoverage.EventsAttachment.SHA256 {
		return nil, nativeGap(h, "native-events-attachment-bytes-differ")
	}
	return raw, nil
}

func validateNativeEventsAttachment(a *Archive, h NativeHistory, scoped bool) error {
	if h.HistoryCoverage == nil {
		return nil
	}
	raw, err := nativeAttachmentBytes(a, h, scoped)
	if err != nil {
		return err
	}
	var original SourceHistory
	if err := decodeJSON(raw, &original); err != nil {
		return err
	}
	events, present := h.Current["events"]
	if original.Format != SourceHistoryFormat || len(original.ProjectHistories) != 0 || original.Range.Status != "unverified" || original.Range.Reason != "native-history-boundary" || original.Range.Complete || original.Range.First != 0 || original.Range.Head != 0 || len(original.Range.Baseline)+len(original.Range.BaselineEvents)+len(original.Range.Frames)+len(original.Range.Gaps) != 0 || !present || h.HistoryCoverage.EventsAttachment.Rows != uint64(len(original.Events)) || len(original.Events) != len(events.Rows) || !sameEventImage(original.Events, events.Rows) || !equalJSON(original.Commits, h.Commits) {
		return nativeGap(h, "native-events-attachment-census-differs")
	}
	portable, err := nativePortableScope(a, h, scoped)
	if err != nil {
		return err
	}
	if portable.Range.Status != "unverified" || portable.Range.Reason != "native-history-boundary" || !sameEventImage(portable.Events, original.Events) || !sameEventImage(portable.Events, currentEventsForProject(a, h.Projects[0])) {
		return nativeGap(h, "native-events-attachment-source-differs")
	}
	return nil
}

func nativePortableScope(a *Archive, h NativeHistory, scoped bool) (ProjectSourceHistory, error) {
	var portable SourceHistory
	if err := decodeJSON(a.members[SourceHistoryMember], &portable); err != nil {
		return ProjectSourceHistory{}, err
	}
	if !scoped {
		return ProjectSourceHistory{Commits: portable.Commits, Events: portable.Events, Range: portable.Range}, nil
	}
	for _, scope := range portable.ProjectHistories {
		if len(h.Projects) == 1 && scope.Project == h.Projects[0] && scope.SourceStoreID == h.StoreID {
			return scope, nil
		}
	}
	return ProjectSourceHistory{}, nativeGap(h, "native-portable-source-scope-missing")
}

func nativeBoundaryChildren(h NativeHistory) map[string][]string {
	children := map[string][]string{}
	for _, commit := range h.Commits {
		for _, parent := range commit.Parents {
			children[parent] = append(children[parent], commit.ID)
		}
	}
	for parent := range children {
		sort.Strings(children[parent])
	}
	return children
}

// NativeHistoryCoverageSHA256 commits the detached descriptor as part of the
// driver-bound payload, in addition to the original archive blob commitment.
func NativeHistoryCoverageSHA256(coverage NativeHistoryCoverage) string {
	raw, _ := json.Marshal(coverage)
	return digest(raw)
}
