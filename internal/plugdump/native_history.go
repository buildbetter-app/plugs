package plugdump

import (
	"bytes"
	"context"
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

// NativeTable retains the actual columns and SQL cells of one observed table.
// Absence from an image means the complete census did not contain that table;
// an empty table is represented with its columns and an explicit empty rows.
type NativeTable struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type NativeHistoryImage struct {
	Sequence uint64                 `json:"sequence"`
	CommitID string                 `json:"commit_id"`
	Tables   map[string]NativeTable `json:"tables"`
}

type NativeHistoryGap struct {
	Sequence uint64 `json:"sequence"`
	CommitID string `json:"commit_id"`
	Family   string `json:"family,omitempty"`
	Reason   string `json:"reason"`
}

// NativeHistory is retained native SQL evidence, independently versioned from
// portable business replay in history/source.json. Complete means every actual
// reachable immutable image was read, not that an old schema can be interpreted
// as today's Plug business model. No unversioned row is assigned to a commit.
type NativeHistory struct {
	Format   string                 `json:"format"`
	StoreID  string                 `json:"store_id"`
	Projects []string               `json:"projects"`
	Head     string                 `json:"head"`
	Commits  []SourceCommit         `json:"commits"`
	Ignore   NativeTable            `json:"ignore"`
	Current  map[string]NativeTable `json:"current"`
	Images   []NativeHistoryImage   `json:"images"`
	Complete bool                   `json:"complete"`
	Gaps     []NativeHistoryGap     `json:"gaps"`
}

// NativeSourceHistoryReader uses the same scoped read transaction as Source.
// It reads historical schemas as observed, without applying today's mapper.
type NativeSourceHistoryReader interface {
	SourceHistoryReader
	ReadNativeSourceImage(context.Context, string) (map[string]Table, error)
	ReadSourceIgnore(context.Context) (Table, error)
}

var nativeSQLName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

var nativeUnversioned = map[string]bool{
	"bd_events_journal": true, "bd_events_seq": true, "events": true,
	"ignored_schema_migrations": true, "leases": true, "local_metadata": true,
	"repo_mtimes": true, "wisp_child_counters": true, "wisp_comments": true,
	"wisp_dependencies": true, "wisp_events": true, "wisp_labels": true, "wisps": true,
}

func validateSourceFamilyMarker(data []byte, contract string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		if key != "source_family_contract" && strings.EqualFold(key, "source_family_contract") {
			return fmt.Errorf("noncanonical source_family_contract")
		}
	}
	if _, present := fields["source_family_contract"]; present && contract != SourceFamilyContract {
		return fmt.Errorf("unsupported source family contract")
	}
	if contract != "" && contract != SourceFamilyContract {
		return fmt.Errorf("unsupported source family contract")
	}
	return nil
}

func (s SQLSource) ReadSourceIgnore(ctx context.Context) (Table, error) {
	if s.DB == nil {
		return Table{}, ErrSourceHistoryUnavailable{Reason: "missing-scoped-reader"}
	}
	return s.historyTable(ctx, "SELECT * FROM dolt_ignore")
}

func (s SQLSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	if !validCommitID(id) || s.DB == nil {
		return nil, ErrSourceHistoryUnavailable{Reason: "invalid-image-request"}
	}
	names, err := s.historyTable(ctx, "SHOW FULL TABLES AS OF '"+id+"'")
	if err != nil || len(names.Columns) != 2 {
		return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unavailable"}
	}
	result := map[string]Table{}
	for _, row := range names.Rows {
		name, kind := historyText(row[names.Columns[0]]), historyText(row[names.Columns[1]])
		if kind == "VIEW" {
			continue
		}
		if kind != "BASE TABLE" || !nativeSQLName.MatchString(name) {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unmappable"}
		}
		if _, exists := result[name]; exists {
			return nil, ErrSourceHistoryUnavailable{Reason: "duplicate-historical-table"}
		}
		// Names are read from this immutable census and constrained above. This
		// raw evidence port neither executes DDL nor treats an old schema as v72.
		table, err := s.historyTable(ctx, "SELECT * FROM `"+name+"` AS OF '"+id+"'")
		if err != nil {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-image-unavailable:" + name}
		}
		result[name] = table
	}
	return result, nil
}

func nativeTable(table Table, family string, policy map[string]string) (NativeTable, error) {
	result := NativeTable{Columns: append([]string{}, table.Columns...), Rows: []Row{}}
	if len(table.Columns) == 0 || len(table.Columns) > 1024 || len(table.Rows) > 100000 {
		return result, ErrSourceHistoryUnavailable{Reason: "native-table-limit:" + family}
	}
	sort.Strings(result.Columns)
	for i, column := range result.Columns {
		if !nativeSQLName.MatchString(column) || i > 0 && column == result.Columns[i-1] {
			return result, fmt.Errorf("invalid source column in %s", family)
		}
	}
	for _, original := range table.Rows {
		if len(original) != len(result.Columns) {
			return result, fmt.Errorf("source row schema mismatch in %s", family)
		}
		row := cloneRow(original)
		for _, column := range result.Columns {
			if _, present := row[column]; !present {
				return result, fmt.Errorf("missing source column in %s", family)
			}
		}
		if family == "config" || family == "metadata" {
			key := text(row, "key")
			if secretKey(key) || policy[family+"/"+key] == "secret" {
				row["value"] = map[string]any{"$secret": family + "/" + key}
			}
		}
		if err := validateRowUTF8(row); err != nil {
			return result, err
		}
		values := map[string]any(row)
		if family == "config" || family == "metadata" || family == "local_metadata" {
			// `key` names a SQL column, not a credential. Inspect the stored
			// setting name and its value, as the ordinary source reader does.
			if err := validateSecretReferences(map[string]any{text(row, "key"): row["value"]}); err != nil {
				return result, err
			}
			values = map[string]any{}
			for column, value := range row {
				if column != "key" && column != "value" {
					values[column] = value
				}
			}
		}
		if err := validateSecretReferences(values); err != nil {
			return result, err
		}
		result.Rows = append(result.Rows, row)
	}
	sort.Slice(result.Rows, func(i, j int) bool {
		a, _ := marshal(result.Rows[i])
		b, _ := marshal(result.Rows[j])
		return bytes.Compare(a, b) < 0
	})
	return result, nil
}

func nativeTables(tables map[string]Table, policy map[string]string) (map[string]NativeTable, error) {
	if tables == nil {
		return nil, fmt.Errorf("missing native census")
	}
	result := map[string]NativeTable{}
	for name, table := range tables {
		if !nativeSQLName.MatchString(name) {
			return nil, fmt.Errorf("unsupported native table name")
		}
		value, err := nativeTable(table, name, policy)
		if err != nil {
			return nil, fmt.Errorf("native family %s: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

// A deterministic parent-before-child traversal retains merge ancestry, too.
// A missing parent is a real named gap, never evidence of a new genesis.
func nativeCommitOrder(log SourceCommitLog) ([]SourceCommit, error) {
	if len(log.Commits) == 0 || len(log.Commits) > 10000 || !validCommitID(log.Head) {
		return nil, ErrSourceHistoryUnavailable{Reason: "commit-census-limit-or-empty"}
	}
	byID := map[string]SourceCommit{}
	for _, c := range log.Commits {
		if !validCommitID(c.ID) || c.Metadata == nil {
			return nil, ErrSourceHistoryUnavailable{Reason: "invalid-commit-census"}
		}
		if _, exists := byID[c.ID]; exists {
			return nil, ErrSourceHistoryUnavailable{Reason: "duplicate-commit"}
		}
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return nil, err
		}
		byID[c.ID] = c
	}
	state := map[string]int{}
	result := []SourceCommit{}
	var visit func(string) error
	visit = func(id string) error {
		c, exists := byID[id]
		if !exists {
			return ErrSourceHistoryUnavailable{Reason: "commit-ancestry-missing:" + id}
		}
		if state[id] == 1 {
			return ErrSourceHistoryUnavailable{Reason: "commit-ancestry-cycle"}
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		parents := append([]string{}, c.Parents...)
		sort.Strings(parents)
		for i, p := range parents {
			if !validCommitID(p) || i > 0 && parents[i-1] == p {
				return ErrSourceHistoryUnavailable{Reason: "invalid-ancestry"}
			}
			if err := visit(p); err != nil {
				return err
			}
		}
		state[id] = 2
		result = append(result, c)
		return nil
	}
	if err := visit(log.Head); err != nil {
		return nil, err
	}
	if len(result) != len(byID) {
		return nil, ErrSourceHistoryUnavailable{Reason: "unreachable-commit-census"}
	}
	return result, nil
}

func ignoredNativeFamily(name string, ignore NativeTable) (bool, error) {
	if !nativeUnversioned[name] {
		return false, nil
	}
	canonical := name
	if strings.HasPrefix(name, "wisp_") {
		canonical = "wisp_%"
	}
	matched := false
	for _, row := range ignore.Rows {
		pattern := text(row, "pattern")
		ignored := fmt.Sprint(row["ignored"])
		if ignored != "true" && ignored != "false" && ignored != "1" && ignored != "0" {
			return false, fmt.Errorf("invalid native ignore value")
		}
		if strings.EqualFold(pattern, name) {
			return ignored == "true" || ignored == "1", nil
		}
	}
	for _, row := range ignore.Rows {
		pattern := text(row, "pattern")
		on := fmt.Sprint(row["ignored"]) == "true" || fmt.Sprint(row["ignored"]) == "1"
		if pattern == canonical && on {
			matched = true
		}
		// Do not guess Dolt's specificity ordering for noncanonical wildcard
		// overrides. Exact-name overrides above are authoritative.
		if !on && strings.ContainsAny(pattern, "%*?\\") {
			return false, fmt.Errorf("ambiguous native ignore policy for %s", name)
		}
	}
	return matched, nil
}

func nativeFamilyHistory(name string, head map[string]NativeTable, ignore NativeTable) (string, error) {
	if _, present := head[name]; present {
		return FamilyVersioned, nil
	}
	ignored, err := ignoredNativeFamily(name, ignore)
	if err != nil {
		return "", err
	}
	if !ignored {
		return "", ErrSourceHistoryUnavailable{Reason: "head-image-missing-versioned-family:" + name}
	}
	return FamilyUnversioned, nil
}

func captureNativeHistory(ctx context.Context, src Source, inventory Inventory, tables map[string]Table) (*NativeHistory, error) {
	port, ok := src.(NativeSourceHistoryReader)
	if !ok {
		return nil, ErrSourceHistoryUnavailable{Reason: "native-history-reader-unavailable"}
	}
	log, err := port.ReadSourceCommits(ctx)
	if err != nil {
		return nil, err
	}
	commits, err := nativeCommitOrder(log)
	if err != nil {
		return nil, err
	}
	if !log.Clean {
		return nil, ErrSourceHistoryUnavailable{Reason: "working-set-not-clean"}
	}
	ignore, err := port.ReadSourceIgnore(ctx)
	if err != nil {
		return nil, err
	}
	ignored, err := nativeTable(ignore, "dolt_ignore", nil)
	if err != nil {
		return nil, err
	}
	current, err := nativeTables(tables, inventory.ConfigPolicy)
	if err != nil {
		return nil, err
	}
	h := &NativeHistory{Format: NativeHistoryFormat, StoreID: inventory.StoreID, Projects: []string{}, Head: log.Head, Commits: commits, Ignore: ignored, Current: current, Images: []NativeHistoryImage{}, Complete: true, Gaps: []NativeHistoryGap{}}
	for _, project := range inventory.Projects {
		h.Projects = append(h.Projects, project.Prefix)
	}
	sort.Strings(h.Projects)
	total := 0
	for i, c := range commits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		image, err := port.ReadNativeSourceImage(ctx, c.ID)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			reason := "historical-image-unavailable"
			if typed, ok := err.(ErrSourceHistoryUnavailable); ok {
				reason = typed.Reason
			}
			family := ""
			if prefix, suffix, found := strings.Cut(reason, ":"); found && prefix == "historical-image-unavailable" {
				reason, family = prefix, suffix
			}
			h.Complete = false
			h.Gaps = append(h.Gaps, NativeHistoryGap{Sequence: uint64(i + 1), CommitID: c.ID, Family: family, Reason: reason})
			continue
		}
		value, err := nativeTables(image, inventory.ConfigPolicy)
		if err != nil {
			return nil, err
		}
		frame := NativeHistoryImage{Sequence: uint64(i + 1), CommitID: c.ID, Tables: value}
		data, err := marshal(frame)
		if err != nil {
			return nil, err
		}
		total += len(data)
		if total > MaxArchiveBytes/2 {
			return nil, ErrSourceHistoryUnavailable{Reason: "native-image-census-limit"}
		}
		h.Images = append(h.Images, frame)
	}
	final, err := port.ReadSourceCommits(ctx)
	if err != nil {
		return nil, err
	}
	ordered, err := nativeCommitOrder(final)
	if err != nil || !final.Clean || final.Head != log.Head || !equalJSON(commits, ordered) {
		return nil, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
	}
	return h, nil
}

func familyAction(name, contract string) (string, string) {
	if contract == SourceFamilyContract && (name == "leases" || name == "repo_mtimes") {
		return "archive", "current SQL rows retained; lease eligibility is evaluated at the supplied restore time"
	}
	return actionFor(name)
}

func nativeHead(h NativeHistory) map[string]NativeTable {
	for _, image := range h.Images {
		if image.CommitID == h.Head {
			return image.Tables
		}
	}
	return nil
}

func classifyNativeSources(sources []Disposition, h NativeHistory) error {
	head := nativeHead(h)
	if head == nil {
		return ErrSourceHistoryUnavailable{Reason: "native-head-image-unavailable"}
	}
	for i := range sources {
		s := &sources[i]
		if _, table := tableColumns[s.Family]; !table {
			continue
		}
		classification, err := nativeFamilyHistory(s.Family, head, h.Ignore)
		if err != nil {
			return err
		}
		s.History = classification
		if classification == FamilyUnversioned {
			s.Reason += "; unversioned in Beads by design"
		}
	}
	return nil
}

func validateNativeHistory(manifest Manifest, members map[string][]byte) error {
	raw, present := members[NativeHistoryMember]
	if manifest.SourceFamilyContract == "" {
		if present {
			return fmt.Errorf("native history requires source family contract")
		}
		for _, s := range manifest.Sources {
			if s.History != "" {
				return fmt.Errorf("family history requires source family contract")
			}
		}
		return nil
	}
	if manifest.SourceFamilyContract != SourceFamilyContract || manifest.SourcePlaneAddressContract != SourcePlaneAddressContract || !present {
		return fmt.Errorf("unsupported or missing native family history")
	}
	var h NativeHistory
	if err := decodeJSON(raw, &h); err != nil {
		return fmt.Errorf("native history shape: %w", err)
	}
	projects := []string{}
	for _, p := range manifest.Projects {
		projects = append(projects, p.Prefix)
	}
	sort.Strings(projects)
	if h.Format != NativeHistoryFormat || h.StoreID != manifest.StoreID || !equalJSON(projects, h.Projects) || h.Current == nil || h.Images == nil || h.Gaps == nil {
		return fmt.Errorf("native history source identity or shape mismatch")
	}
	ordered, err := nativeCommitOrder(SourceCommitLog{Head: h.Head, Clean: true, Commits: h.Commits})
	if err != nil {
		return err
	}
	if !equalJSON(ordered, h.Commits) {
		return fmt.Errorf("unordered native commit graph")
	}
	validateTables := func(tables map[string]NativeTable) error {
		if tables == nil {
			return fmt.Errorf("missing native table census")
		}
		for name, table := range tables {
			if !nativeSQLName.MatchString(name) || table.Columns == nil || table.Rows == nil {
				return fmt.Errorf("invalid native table census")
			}
			value, err := nativeTable(Table{Columns: table.Columns, Rows: table.Rows}, name, nil)
			if err != nil || !equalJSON(value, table) {
				return fmt.Errorf("invalid or noncanonical native family %s", name)
			}
		}
		return nil
	}
	if err := validateTables(map[string]NativeTable{"dolt_ignore": h.Ignore}); err != nil {
		return err
	}
	if !equalJSON(h.Ignore.Columns, []string{"ignored", "pattern"}) {
		return fmt.Errorf("invalid native ignore schema")
	}
	if err := validateTables(h.Current); err != nil {
		return err
	}
	if len(h.Current) != len(tableColumns) {
		return fmt.Errorf("incomplete native current census")
	}
	for name := range tableColumns {
		columns := columnsFor(name)
		sort.Strings(columns)
		if !equalJSON(h.Current[name].Columns, columns) {
			return fmt.Errorf("native current schema drift in %s", name)
		}
	}
	covered := map[uint64]bool{}
	last := uint64(0)
	for _, image := range h.Images {
		if image.Sequence == 0 || image.Sequence > uint64(len(ordered)) || image.Sequence <= last || image.CommitID != ordered[image.Sequence-1].ID {
			return fmt.Errorf("invalid native image sequence")
		}
		last = image.Sequence
		covered[last] = true
		if err := validateTables(image.Tables); err != nil {
			return err
		}
	}
	last = 0
	for _, gap := range h.Gaps {
		if gap.Sequence == 0 || gap.Sequence > uint64(len(ordered)) || gap.Sequence <= last || covered[gap.Sequence] || gap.CommitID != ordered[gap.Sequence-1].ID || strings.TrimSpace(gap.Reason) == "" {
			return fmt.Errorf("invalid native named gap")
		}
		last = gap.Sequence
		covered[last] = true
	}
	if len(covered) != len(ordered) || h.Complete != (len(h.Gaps) == 0) {
		return fmt.Errorf("native image census incomplete without named gap")
	}
	head := nativeHead(h)
	if head == nil {
		return ErrSourceHistoryUnavailable{Reason: "native-head-image-unavailable"}
	}
	for _, s := range manifest.Sources {
		if _, table := tableColumns[s.Family]; !table {
			if s.History != "" {
				return fmt.Errorf("non-table family has native classification")
			}
			continue
		}
		classification, err := nativeFamilyHistory(s.Family, head, h.Ignore)
		if err != nil {
			return err
		}
		if s.History != classification || s.Rows != len(h.Current[s.Family].Rows) {
			return fmt.Errorf("native family classification/count mismatch in %s", s.Family)
		}
		if classification == FamilyUnversioned && !strings.Contains(s.Reason, "unversioned in Beads by design") {
			return fmt.Errorf("native family is missing explicit unversioned label")
		}
		if classification == FamilyVersioned && !equalJSON(head[s.Family], h.Current[s.Family]) {
			return ErrSourceHistoryUnavailable{Reason: "head-image-differs-versioned-family:" + s.Family}
		}
	}
	// Bind current native SQL evidence to the original typed archive records,
	// including source rows and every normalized label. No count-only witness.
	archived := map[string][]Row{}
	for _, kind := range manifest.Kinds {
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			var r Record
			if err := decodeJSON(line, &r); err != nil {
				return err
			}
			archived[r.Source.Table] = append(archived[r.Source.Table], r.Source.Fields)
			if r.Kind == "bead" {
				for _, label := range r.Body["labels"].([]any) {
					archived["labels"] = append(archived["labels"], Row{"issue_id": r.ID, "label": label})
				}
			}
		}
	}
	for name, current := range h.Current {
		columns := append([]string{}, current.Columns...)
		rows := []Row{}
		if name == "issues" {
			columns = []string{}
			for _, c := range current.Columns {
				if c != "row_lock" && c != "is_blocked" {
					columns = append(columns, c)
				}
			}
		}
		for _, row := range current.Rows {
			copy := cloneRow(row)
			if name == "issues" {
				delete(copy, "row_lock")
				delete(copy, "is_blocked")
			}
			rows = append(rows, copy)
		}
		expected, err := nativeTable(Table{Columns: columns, Rows: rows}, name, nil)
		if err != nil {
			return err
		}
		actual, err := nativeTable(Table{Columns: columns, Rows: archived[name]}, name, nil)
		if err != nil {
			return err
		}
		if !equalJSON(expected, actual) {
			return fmt.Errorf("native current/archive rows differ in %s", name)
		}
	}
	return nil
}
