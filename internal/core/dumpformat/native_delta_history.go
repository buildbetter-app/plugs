package dumpformat

import (
	"fmt"
	"sort"
)

// FamilyOpaqueNative preserves physical system-table evidence without making
// its SQL definitions or rows active in Plug.
const FamilyOpaqueNative = "opaque-native-physical"

// OpaqueNativeCurrentFamily identifies the exact physical system families of
// an authenticated v2 history. They are outside the portable BASE TABLE census;
// their original typed rows remain authenticated by the native stream.
func (h NativeHistory) OpaqueNativeCurrentFamily(name string) bool {
	if !h.deltaVerified {
		return false
	}
	if _, present := h.Current[name]; !present {
		return false
	}
	switch name {
	case "dolt_ignore", "dolt_nonlocal_tables", "dolt_schemas":
		return true
	}
	return false
}

func checkedNativeCommitCensus(h NativeHistory) error {
	if !h.deltaVerified {
		return nativeCommitCensus(h)
	}
	byID := map[string]SourceCommit{}
	frontier := map[string]NativeHistoryBoundary{}
	for _, c := range h.Commits {
		if !validCommitID(c.ID) || c.Parents == nil || c.Metadata == nil {
			return fmt.Errorf("invalid native delta graph node")
		}
		if _, ok := byID[c.ID]; ok {
			return fmt.Errorf("duplicate native delta graph node")
		}
		byID[c.ID] = c
	}
	if h.HistoryCoverage != nil {
		children := nativeBoundaryChildren(h)
		for _, b := range h.HistoryCoverage.Boundaries {
			if _, ok := byID[b.CommitID]; ok || !equalJSON(children[b.CommitID], b.ChildCommitIDs) {
				return fmt.Errorf("native delta boundary edges differ")
			}
			frontier[b.CommitID] = b
		}
	}
	state := map[string]bool{}
	extended := []string{}
	var visit func(string) error
	visit = func(id string) error {
		if state[id] {
			return nil
		}
		c, ok := byID[id]
		if !ok {
			if _, ok := frontier[id]; !ok {
				return fmt.Errorf("native delta graph parent missing")
			}
			state[id] = true
			extended = append(extended, id)
			return nil
		}
		state[id] = true
		for _, p := range c.Parents {
			if err := visit(p); err != nil {
				return err
			}
		}
		extended = append(extended, id)
		return nil
	}
	if err := visit(h.Head); err != nil {
		return err
	}
	if len(state) != len(byID)+len(frontier) {
		return fmt.Errorf("native delta graph contains unreachable nodes")
	}
	// Ordinals retain the existing CP11 sorted-parent traversal coordinates;
	// actual first-parent order is independently preserved by v2 graph records.
	if err := checkNativeDeltaBoundaryOrdinals(h, byID, frontier); err != nil {
		return err
	}
	return nil
}

func checkNativeDeltaBoundaryOrdinals(h NativeHistory, byID map[string]SourceCommit, frontier map[string]NativeHistoryBoundary) error {
	seen := map[string]bool{}
	order := []string{}
	var walk func(string)
	walk = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		c, ok := byID[id]
		if ok {
			parents := append([]string{}, c.Parents...)
			sortNativeDeltaStrings(parents)
			for _, p := range parents {
				walk(p)
			}
		}
		order = append(order, id)
	}
	walk(h.Head)
	for i, id := range order {
		if b, ok := frontier[id]; ok && b.CommitOrdinalFromHead != uint64(len(order)-i) {
			return fmt.Errorf("native delta boundary ordinal differs")
		}
	}
	return nil
}

func sortNativeDeltaStrings(s []string) { sort.Strings(s) }

// The portable v1 attachment witnesses a parent set; the v2 stream separately
// authenticates actual ordered parents and the exact first-parent delta base.
func sameNativeDeltaCommitCensus(left, right []SourceCommit) bool {
	if len(left) != len(right) {
		return false
	}
	byID := map[string]SourceCommit{}
	for _, c := range left {
		if _, ok := byID[c.ID]; ok {
			return false
		}
		byID[c.ID] = c
	}
	for _, c := range right {
		other, ok := byID[c.ID]
		if !ok {
			return false
		}
		delete(byID, c.ID)
		a, b := append([]string{}, c.Parents...), append([]string{}, other.Parents...)
		sort.Strings(a)
		sort.Strings(b)
		if !equalJSON(a, b) || !equalJSON(c.Metadata, other.Metadata) {
			return false
		}
	}
	return len(byID) == 0
}

// Copy directly, without serializing the aggregate history or widening legacy
// decoder limits. The receiver budget already charges reconstructed images.
func cloneNativeDeltaHistory(h NativeHistory) NativeHistory {
	copy := h
	copy.Projects = append([]string{}, h.Projects...)
	copy.Commits = append([]SourceCommit{}, h.Commits...)
	for i := range copy.Commits {
		copy.Commits[i].Parents = append([]string{}, h.Commits[i].Parents...)
		copy.Commits[i].Metadata = cloneRow(h.Commits[i].Metadata)
	}
	copy.Ignore = cloneNativeDeltaTable(h.Ignore)
	copy.Current = map[string]NativeTable{}
	for k, v := range h.Current {
		copy.Current[k] = cloneNativeDeltaTable(v)
	}
	copy.Images = make([]NativeHistoryImage, len(h.Images))
	for i, image := range h.Images {
		copy.Images[i] = image
		copy.Images[i].Tables = map[string]NativeTable{}
		for k, v := range image.Tables {
			copy.Images[i].Tables[k] = cloneNativeDeltaTable(v)
		}
		if image.SchemaProfile != nil {
			p := *image.SchemaProfile
			p.MainVersion = cloneNativeDeltaCursor(p.MainVersion)
			p.IgnoredVersion = cloneNativeDeltaCursor(p.IgnoredVersion)
			p.Absences = append([]NativeCommitSchemaAbsence{}, p.Absences...)
			copy.Images[i].SchemaProfile = &p
		}
	}
	copy.Sources = append([]Disposition{}, h.Sources...)
	for i := range copy.Sources {
		if copy.Sources[i].Identity != nil {
			value := *copy.Sources[i].Identity
			copy.Sources[i].Identity = &value
		}
	}
	copy.Gaps = append([]NativeHistoryGap{}, h.Gaps...)
	if h.SchemaProfile != nil {
		p := *h.SchemaProfile
		p.Absences = append([]NativeSchemaAbsence{}, p.Absences...)
		copy.SchemaProfile = &p
	}
	if h.HistoryCoverage != nil {
		p := *h.HistoryCoverage
		p.Boundaries = append([]NativeHistoryBoundary{}, p.Boundaries...)
		for i := range p.Boundaries {
			p.Boundaries[i].CommitSchemaVersion = cloneNativeDeltaCursor(p.Boundaries[i].CommitSchemaVersion)
			p.Boundaries[i].ChildCommitIDs = append([]string{}, p.Boundaries[i].ChildCommitIDs...)
			p.Boundaries[i].SQLDoltCodes = append([]uint16(nil), p.Boundaries[i].SQLDoltCodes...)
		}
		copy.HistoryCoverage = &p
	}
	return copy
}

func cloneNativeDeltaCursor(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func cloneNativeDeltaTable(t NativeTable) NativeTable {
	out := NativeTable{Columns: append([]string{}, t.Columns...), Rows: make([]Row, len(t.Rows))}
	for i, row := range t.Rows {
		out.Rows[i] = cloneRow(row)
	}
	return out
}
