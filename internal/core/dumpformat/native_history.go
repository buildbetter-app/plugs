package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const SourceFamilyContract = "beads-source-families/1"
const NativeHistoryFormat = "beads-native-tables/1"
const NativeHistoryMember = "history/native-tables.json"

const (
	FamilyVersioned   = "versioned"
	FamilyUnversioned = "unversioned-in-beads-by-design"
)

// NativeTable preserves actual SQL columns and cells, including duplicate rows,
// SQL NULL, lossless decimal numbers and binary envelopes. Historical schemas
// are not interpreted as today's portable business records.
type NativeTable struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type NativeHistoryImage struct {
	Sequence      uint64                     `json:"sequence"`
	CommitID      string                     `json:"commit_id"`
	Tables        map[string]NativeTable     `json:"tables"`
	SchemaProfile *NativeCommitSchemaProfile `json:"schema_profile,omitempty"`
}

type NativeHistoryGap struct {
	Sequence uint64 `json:"sequence"`
	CommitID string `json:"commit_id"`
	Family   string `json:"family,omitempty"`
	Reason   string `json:"reason"`
}

// NativeHistory is independently validated source evidence. Complete does not
// promote history/source.json, reconstruct portable bodies or prove publication.
// Sources is a detached API view of this source's original family census; it is
// not a field in the producer's beads-native-tables/1 wire.
type NativeHistory struct {
	Format          string                 `json:"format"`
	StoreID         string                 `json:"store_id"`
	Projects        []string               `json:"projects"`
	Head            string                 `json:"head"`
	Commits         []SourceCommit         `json:"commits"`
	Ignore          NativeTable            `json:"ignore"`
	Current         map[string]NativeTable `json:"current"`
	Images          []NativeHistoryImage   `json:"images"`
	Complete        bool                   `json:"complete"`
	Gaps            []NativeHistoryGap     `json:"gaps"`
	SchemaProfile   *NativeSchemaProfile   `json:"schema_profile,omitempty"`
	HistoryCoverage *NativeHistoryCoverage `json:"history_coverage,omitempty"`
	Sources         []Disposition          `json:"-"`
}

func nativeObject(data []byte, required string, optional string) (map[string]any, error) {
	var object map[string]any
	if err := decodeJSON(data, &object); err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, key := range strings.Fields(required) {
		allowed[key] = true
		if object[key] == nil {
			return nil, fmt.Errorf("native history requires %s", key)
		}
	}
	for _, key := range strings.Fields(optional) {
		allowed[key] = true
	}
	for key, value := range object {
		if !allowed[key] || value == nil {
			return nil, fmt.Errorf("unknown, null or noncanonical native field %s", key)
		}
	}
	return object, nil
}

func (h *NativeHistory) UnmarshalJSON(data []byte) error {
	object, err := nativeObject(data, "format store_id projects head commits ignore current images complete gaps", "schema_profile history_coverage")
	if err != nil {
		return err
	}
	commits, ok := object["commits"].([]any)
	if !ok {
		return fmt.Errorf("invalid native commit census")
	}
	for _, c := range commits {
		raw, _ := json.Marshal(c)
		if _, err := nativeObject(raw, "id parents metadata", ""); err != nil {
			return err
		}
	}
	type plain NativeHistory
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*h = NativeHistory(value)
	return nil
}

func (t *NativeTable) UnmarshalJSON(data []byte) error {
	if _, err := nativeObject(data, "columns rows", ""); err != nil {
		return err
	}
	type plain NativeTable
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*t = NativeTable(value)
	return nil
}

func (i *NativeHistoryImage) UnmarshalJSON(data []byte) error {
	if _, err := nativeObject(data, "sequence commit_id tables", "schema_profile"); err != nil {
		return err
	}
	type plain NativeHistoryImage
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*i = NativeHistoryImage(value)
	return nil
}

func (g *NativeHistoryGap) UnmarshalJSON(data []byte) error {
	object, err := nativeObject(data, "sequence commit_id reason", "family")
	if err != nil {
		return err
	}
	if family, present := object["family"]; present && family == "" {
		return fmt.Errorf("empty native gap family")
	}
	type plain NativeHistoryGap
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*g = NativeHistoryGap(value)
	return nil
}

var nativeSQLName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

var nativeUnversioned = map[string]bool{
	SourceEventJournalTable: true, SourceEventSequenceTable: true, "events": true,
	"ignored_schema_migrations": true, "leases": true, "local_metadata": true,
	"repo_mtimes": true, EphemeralChildCounterTable: true, EphemeralCommentTable: true,
	EphemeralDependencyTable: true, EphemeralEventTable: true, EphemeralLabelTable: true, EphemeralEntityTable: true,
}

func nativeHistoryPath(project string) string { return "history/native-tables/" + project + ".json" }

func scopedNativeMember(name string) (string, bool) {
	const prefix = "history/native-tables/"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".json") {
		return "", false
	}
	project := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".json")
	return project, prefixPattern.MatchString(project)
}

func validateSourceFamilyMarker(fields map[string]json.RawMessage, m Manifest) error {
	for key := range fields {
		if key != "source_family_contract" && strings.EqualFold(key, "source_family_contract") {
			return fmt.Errorf("noncanonical source_family_contract")
		}
	}
	if _, present := fields["source_family_contract"]; present && m.SourceFamilyContract != SourceFamilyContract {
		return fmt.Errorf("unsupported source family contract")
	}
	if m.SourceFamilyContract != "" && (m.Scope != Scope || m.SourcePlaneAddressContract != SourcePlaneAddressContract) {
		return fmt.Errorf("source family contract requires offline source planes")
	}
	return nil
}

func validateNativeTable(name string, table NativeTable) error {
	if !nativeSQLName.MatchString(name) || len(table.Columns) == 0 || len(table.Columns) > 1024 || table.Rows == nil || len(table.Rows) > 100000 {
		return fmt.Errorf("invalid native table census for %s", name)
	}
	for i, column := range table.Columns {
		if !nativeSQLName.MatchString(column) || i > 0 && column <= table.Columns[i-1] {
			return fmt.Errorf("invalid native columns for %s", name)
		}
	}
	var previous []byte
	for _, row := range table.Rows {
		if len(row) != len(table.Columns) {
			return fmt.Errorf("native row schema differs in %s", name)
		}
		for _, column := range table.Columns {
			if _, ok := row[column]; !ok {
				return fmt.Errorf("native row lacks %s.%s", name, column)
			}
		}
		values := map[string]any(row)
		if name == "config" || name == "metadata" || name == "local_metadata" {
			if err := validateSecretReferences(map[string]any{stringValue(row["key"]): row["value"]}); err != nil {
				return err
			}
			values = map[string]any{}
			for column, value := range row {
				if column != "key" && column != "value" {
					values[column] = value
				}
			}
		}
		if err := validateSecretReferences(values); err != nil {
			return err
		}
		data, err := json.Marshal(row)
		if err != nil || previous != nil && bytes.Compare(previous, data) > 0 {
			return fmt.Errorf("unordered native rows in %s", name)
		}
		previous = data
	}
	return nil
}

func validateNativeTables(tables map[string]NativeTable) error {
	if tables == nil {
		return fmt.Errorf("missing native table census")
	}
	for name, table := range tables {
		if err := validateNativeTable(name, table); err != nil {
			return err
		}
	}
	return nil
}

func nativeGap(h NativeHistory, reason string) IncompleteHistoryError {
	project := ""
	if len(h.Projects) == 1 {
		project = h.Projects[0]
	}
	return IncompleteHistoryError{StoreID: h.StoreID, Project: project, Reason: reason}
}

func requireCompleteNativeHistory(h NativeHistory) error {
	if h.Complete && len(h.Gaps) == 0 {
		return nil
	}
	gap := nativeGap(h, "complete native history unavailable")
	if len(h.Gaps) != 0 {
		g := h.Gaps[0]
		gap.First, gap.Last, gap.Reason, gap.CommitID, gap.Family = g.Sequence, g.Sequence, g.Reason, g.CommitID, g.Family
	}
	return gap
}

func nativeCommitCensus(h NativeHistory) error {
	if len(h.Commits) == 0 || len(h.Commits) > 10000 || !validCommitID(h.Head) {
		return nativeGap(h, "commit-census-limit-or-empty")
	}
	byID := map[string]SourceCommit{}
	for _, c := range h.Commits {
		if !validCommitID(c.ID) || c.Parents == nil || c.Metadata == nil {
			return nativeGap(h, "invalid-commit-census")
		}
		if _, exists := byID[c.ID]; exists {
			return nativeGap(h, "duplicate-commit:"+c.ID)
		}
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return err
		}
		byID[c.ID] = c
	}
	frontier := map[string]NativeHistoryBoundary{}
	if h.HistoryCoverage != nil {
		children := nativeBoundaryChildren(h)
		for _, boundary := range h.HistoryCoverage.Boundaries {
			if _, internal := byID[boundary.CommitID]; internal || !equalJSON(children[boundary.CommitID], boundary.ChildCommitIDs) {
				return nativeGap(h, "native-boundary-edge-census-differs")
			}
			frontier[boundary.CommitID] = boundary
		}
	}
	state, ordered := map[string]int{}, []string{}
	extended := []string{}
	var visit func(string) error
	visit = func(id string) error {
		c, exists := byID[id]
		if !exists {
			if _, boundary := frontier[id]; boundary {
				if state[id] == 0 {
					state[id] = 2
					extended = append(extended, id)
				}
				return nil
			}
			return nativeGap(h, "commit-ancestry-missing:"+id)
		}
		if state[id] == 1 {
			return nativeGap(h, "commit-ancestry-cycle:"+id)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		parents := append([]string{}, c.Parents...)
		sort.Strings(parents)
		for i, p := range parents {
			if !validCommitID(p) || i > 0 && p == parents[i-1] {
				return nativeGap(h, "invalid-ancestry:"+id)
			}
			if err := visit(p); err != nil {
				return err
			}
		}
		state[id] = 2
		ordered = append(ordered, id)
		extended = append(extended, id)
		return nil
	}
	if err := visit(h.Head); err != nil {
		return err
	}
	if len(ordered) != len(h.Commits) {
		return nativeGap(h, "unreachable-commit-census")
	}
	for i, id := range ordered {
		if h.Commits[i].ID != id {
			return nativeGap(h, "unordered-commit-census")
		}
	}
	for _, boundary := range frontier {
		if state[boundary.CommitID] != 2 {
			return nativeGap(h, "unreachable-native-boundary")
		}
	}
	for i, id := range extended {
		if boundary, present := frontier[id]; present && boundary.CommitOrdinalFromHead != uint64(len(extended)-i) {
			return nativeGap(h, "native-boundary-ordinal-differs")
		}
	}
	return nil
}

func nativeFamilyHistory(name string, head map[string]NativeTable, ignore NativeTable) (string, error) {
	if _, present := head[name]; present {
		return FamilyVersioned, nil
	}
	if !nativeUnversioned[name] {
		return "", fmt.Errorf("head-image-missing-versioned-family:%s", name)
	}
	for _, row := range ignore.Rows {
		if strings.EqualFold(stringValue(row["pattern"]), name) {
			if fmt.Sprint(row["ignored"]) == "true" || fmt.Sprint(row["ignored"]) == "1" {
				return FamilyUnversioned, nil
			}
			return "", fmt.Errorf("head-image-missing-versioned-family:%s", name)
		}
	}
	canonical, matched := name, false
	prefix := EphemeralEntityTable[:len(EphemeralEntityTable)-1] + "_"
	if strings.HasPrefix(name, prefix) {
		canonical = prefix + "%"
	}
	for _, row := range ignore.Rows {
		pattern := stringValue(row["pattern"])
		on := fmt.Sprint(row["ignored"]) == "true" || fmt.Sprint(row["ignored"]) == "1"
		if !on && strings.ContainsAny(pattern, "%*?\\") {
			return "", fmt.Errorf("ambiguous native ignore policy for %s", name)
		}
		matched = matched || on && pattern == canonical
	}
	if matched {
		return FamilyUnversioned, nil
	}
	return "", fmt.Errorf("head-image-missing-versioned-family:%s", name)
}

func validateNativeHistory(h NativeHistory) error {
	columns, schemaErr := validateNativeSchema(h)
	if schemaErr != nil {
		return schemaErr
	}
	if h.Format != NativeHistoryFormat || h.StoreID == "" || len(h.Projects) == 0 || h.Current == nil || h.Images == nil || h.Gaps == nil || len(h.Images) > 10000 || len(h.Gaps) > 10000 {
		return fmt.Errorf("invalid native history identity or shape")
	}
	for i, p := range h.Projects {
		if !validSourceIdentity(SourceIdentity{StoreID: h.StoreID, Project: p}) || !prefixPattern.MatchString(p) || i > 0 && p <= h.Projects[i-1] {
			return fmt.Errorf("invalid native source project identity")
		}
	}
	if err := validateNativeCoverageShape(h); err != nil {
		return err
	}
	if err := nativeCommitCensus(h); err != nil {
		return err
	}
	if err := validateNativeTable("dolt_ignore", h.Ignore); err != nil {
		return err
	}
	if !equalJSON(h.Ignore.Columns, []string{"ignored", "pattern"}) {
		return fmt.Errorf("invalid native ignore schema")
	}
	for _, row := range h.Ignore.Rows {
		v := fmt.Sprint(row["ignored"])
		if _, ok := row["pattern"].(string); !ok || v != "true" && v != "false" && v != "1" && v != "0" {
			return fmt.Errorf("invalid native ignore value")
		}
	}
	if err := validateNativeTables(h.Current); err != nil {
		return err
	}
	if len(h.Current) != len(columns) {
		return fmt.Errorf("incomplete native current family census")
	}
	for name, cols := range columns {
		columns := strings.Fields(cols)
		sort.Strings(columns)
		if !equalJSON(h.Current[name].Columns, columns) {
			return nativeGap(h, "native-current-schema-differs:"+name)
		}
	}
	covered, last := map[uint64]bool{}, uint64(0)
	var head map[string]NativeTable
	for _, image := range h.Images {
		if image.Sequence == 0 || image.Sequence > uint64(len(h.Commits)) || image.Sequence <= last || image.CommitID != h.Commits[image.Sequence-1].ID {
			return fmt.Errorf("invalid native image sequence")
		}
		last, covered[image.Sequence] = image.Sequence, true
		if err := validateNativeTables(image.Tables); err != nil {
			return err
		}
		if image.CommitID == h.Head {
			head = image.Tables
		}
	}
	if err := validateNativeCommitSchemas(h); err != nil {
		return err
	}
	last = 0
	for _, gap := range h.Gaps {
		if gap.Sequence == 0 || gap.Sequence > uint64(len(h.Commits)) || gap.Sequence <= last || covered[gap.Sequence] || gap.CommitID != h.Commits[gap.Sequence-1].ID || strings.TrimSpace(gap.Reason) == "" || gap.Family != "" && !nativeSQLName.MatchString(gap.Family) {
			return fmt.Errorf("invalid native named gap")
		}
		last, covered[gap.Sequence] = gap.Sequence, true
	}
	if len(covered) != len(h.Commits) || h.HistoryCoverage == nil && h.Complete != (len(h.Gaps) == 0) {
		return fmt.Errorf("native commit image or named gap missing")
	}
	if head == nil {
		if h.HistoryCoverage != nil {
			return nativeGap(h, "native-boundary-head-image-missing")
		}
		return requireCompleteNativeHistory(h)
	}
	if h.SchemaProfile != nil {
		for _, absence := range h.SchemaProfile.Absences {
			if absence.Column == "" {
				if _, present := head[absence.Family]; present {
					return NativeSchemaError{h.SchemaProfile.MainVersion, h.SchemaProfile.IgnoredVersion, "absent family present at source head: " + absence.Family}
				}
			}
		}
	}
	seen := map[string]bool{}
	for _, s := range h.Sources {
		if _, ok := columns[s.Family]; !ok {
			if s.History != "" {
				return fmt.Errorf("non-table family has native history classification")
			}
			continue
		}
		classification, err := nativeFamilyHistory(s.Family, head, h.Ignore)
		if err != nil {
			return nativeGap(h, err.Error())
		}
		if seen[s.Family] || s.History != classification || s.Rows != len(h.Current[s.Family].Rows) {
			return fmt.Errorf("native family classification/count mismatch: %s", s.Family)
		}
		seen[s.Family] = true
		if classification == FamilyUnversioned && !strings.Contains(s.Reason, "unversioned in Beads by design") {
			return fmt.Errorf("missing explicit native unversioned label")
		}
		if classification == FamilyVersioned && !equalJSON(head[s.Family], h.Current[s.Family]) {
			return nativeGap(h, "head-image-differs-versioned-family:"+s.Family)
		}
	}
	if len(seen) != len(columns) {
		return fmt.Errorf("native source family census missing")
	}
	return nil
}

func bindNativeCurrent(h NativeHistory, rows map[string][]Record, scoped bool) error {
	archived := map[string][]Row{}
	for _, records := range rows {
		for _, r := range records {
			if scoped && (r.Source.Identity == nil || r.Source.Identity.StoreID != h.StoreID || len(h.Projects) != 1 || r.Source.Identity.Project != h.Projects[0]) {
				continue
			}
			archived[r.Source.Table] = append(archived[r.Source.Table], r.Source.Fields)
			if r.Kind == "bead" {
				labels, _ := r.Body["labels"].([]any)
				for _, label := range labels {
					archived["labels"] = append(archived["labels"], Row{"issue_id": r.ID, "label": label})
				}
			}
		}
	}
	for name, table := range h.Current {
		expected, actual := []Row{}, append([]Row{}, archived[name]...)
		for _, row := range table.Rows {
			copy := Row{}
			for column, value := range row {
				if name != "issues" || column != "row_lock" && column != "is_blocked" {
					copy[column] = value
				}
			}
			expected = append(expected, copy)
		}
		canonical := func(rows []Row) {
			sort.Slice(rows, func(i, j int) bool {
				a, _ := json.Marshal(rows[i])
				b, _ := json.Marshal(rows[j])
				return bytes.Compare(a, b) < 0
			})
		}
		canonical(expected)
		canonical(actual)
		if !equalJSON(expected, actual) {
			return fmt.Errorf("native current/archive rows differ in %s", name)
		}
	}
	return nil
}

func validateNativeSourceHistories(a *Archive) error {
	paths := []string{}
	hasAttachments := false
	for path := range a.members {
		if path == NativeHistoryMember {
			paths = append(paths, path)
		} else if _, ok := scopedNativeMember(path); ok {
			paths = append(paths, path)
		} else if _, ok := scopedNativeSourceAttachment(path); ok {
			hasAttachments = true
		}
	}
	sort.Strings(paths)
	if a.manifest.SourceFamilyContract == "" {
		if len(paths) != 0 || hasAttachments {
			return fmt.Errorf("native history requires source family contract")
		}
		for _, s := range a.manifest.Sources {
			if s.History != "" || s.Identity != nil {
				return fmt.Errorf("native source disposition requires explicit contract")
			}
		}
		return nil
	}
	if len(paths) == 0 {
		return fmt.Errorf("source family contract requires native history")
	}
	scoped := a.manifest.SourceIdentityContract == SourceIdentityContract
	seen := map[string]bool{}
	for _, path := range paths {
		var h NativeHistory
		if err := decodeJSON(a.members[path], &h); err != nil {
			return fmt.Errorf("native history shape: %w", err)
		}
		main, ignored := 72, 28
		if h.SchemaProfile != nil {
			main, ignored = h.SchemaProfile.MainVersion, h.SchemaProfile.IgnoredVersion
		}
		if main != a.manifest.MainVersion || ignored != a.manifest.IgnoredVersion {
			return NativeSchemaError{a.manifest.MainVersion, a.manifest.IgnoredVersion, "manifest and native schema profile differ"}
		}
		if scoped {
			project, ok := scopedNativeMember(path)
			if !ok || len(h.Projects) != 1 || h.Projects[0] != project || !projectInManifest(project, a.manifest.Projects) || seen[project] {
				return fmt.Errorf("native project member identity mismatch")
			}
			seen[project] = true
			identity := SourceIdentity{StoreID: h.StoreID, Project: project}
			for _, s := range a.manifest.Sources {
				if s.Identity != nil && *s.Identity == identity {
					h.Sources = append(h.Sources, s)
				}
			}
			for _, records := range a.byKind {
				for _, r := range records {
					if r.Source.Identity.Project == project && *r.Source.Identity != identity {
						return fmt.Errorf("native record source store differs")
					}
				}
			}
		} else {
			projects := []string{}
			for _, p := range a.manifest.Projects {
				projects = append(projects, p.Prefix)
			}
			sort.Strings(projects)
			if path != NativeHistoryMember || len(paths) != 1 || h.StoreID != a.manifest.StoreID || !equalJSON(projects, h.Projects) {
				return fmt.Errorf("native source identity mismatch")
			}
			h.Sources = append([]Disposition{}, a.manifest.Sources...)
			for _, s := range h.Sources {
				if s.Identity != nil {
					return fmt.Errorf("source disposition identity requires explicit namespace")
				}
			}
		}
		if err := validateNativeHistory(h); err != nil {
			return err
		}
		if err := bindNativePortableCensus(a, h, scoped); err != nil {
			return err
		}
		if err := bindNativeCurrent(h, a.byKind, scoped); err != nil {
			return err
		}
		if err := validateNativeEventsAttachment(a, h, scoped); err != nil {
			return err
		}
		if scoped {
			if err := RequireVerifiedNativeHistory(h); err != nil {
				return err
			}
		}
		a.nativeHistories = append(a.nativeHistories, h)
	}
	for path := range a.members {
		if project, attachment := scopedNativeSourceAttachment(path); attachment {
			found := false
			for _, h := range a.nativeHistories {
				found = found || scoped && h.HistoryCoverage != nil && len(h.Projects) == 1 && h.Projects[0] == project
			}
			if !found {
				return fmt.Errorf("orphan native events attachment")
			}
		}
	}
	if scoped && len(seen) != len(a.manifest.Projects) {
		return fmt.Errorf("missing native source project history")
	}
	return nil
}

// Both members witness the same retained source graph, even when the portable
// projection is unverified. Ordering of equivalent parent sets is immaterial.
func bindNativePortableCensus(a *Archive, h NativeHistory, scoped bool) error {
	mismatch := nativeGap(h, "native-portable-commit-census-mismatch")
	var portable SourceHistory
	if err := decodeJSON(a.members[SourceHistoryMember], &portable); err != nil {
		return mismatch
	}
	commits := portable.Commits
	if scoped {
		commits = nil
		for _, source := range portable.ProjectHistories {
			if len(h.Projects) == 1 && source.Project == h.Projects[0] && source.SourceStoreID == h.StoreID {
				if commits != nil {
					return mismatch
				}
				commits = source.Commits
			}
		}
	}
	if len(commits) != len(h.Commits) {
		return mismatch
	}
	byID := map[string]SourceCommit{}
	for _, commit := range commits {
		if _, exists := byID[commit.ID]; exists {
			return mismatch
		}
		byID[commit.ID] = commit
	}
	for _, commit := range h.Commits {
		other, exists := byID[commit.ID]
		parents, otherParents := append([]string{}, commit.Parents...), append([]string{}, other.Parents...)
		sort.Strings(parents)
		sort.Strings(otherParents)
		if !exists || !equalJSON(parents, otherParents) || !equalJSON(commit.Metadata, other.Metadata) {
			return mismatch
		}
	}
	return nil
}

// NativeSourceHistories returns deterministic detached original source views.
// No raw-table completeness claim changes portable SourceHistory state.
func (a *Archive) NativeSourceHistories() []NativeHistory {
	result := []NativeHistory{}
	if a == nil {
		return result
	}
	for _, h := range a.nativeHistories {
		data, _ := json.Marshal(h)
		var value NativeHistory
		_ = decodeJSON(data, &value)
		data, _ = json.Marshal(h.Sources)
		_ = decodeJSON(data, &value.Sources)
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StoreID != result[j].StoreID {
			return result[i].StoreID < result[j].StoreID
		}
		return strings.Join(result[i].Projects, "\x00") < strings.Join(result[j].Projects, "\x00")
	})
	return result
}
