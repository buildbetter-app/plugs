package main

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// These are the existing reader's evidence wire types. SQL types and driver
// kinds are observations; normalized archive values cannot supply either.
type typedCell struct {
	Kind  string `json:"kind"`
	Value string `json:"value,omitempty"`
}
type typedColumn struct {
	Name    string `json:"name"`
	SQLType string `json:"sql_type"`
}
type typedTable struct {
	Name    string        `json:"name"`
	Columns []typedColumn `json:"columns"`
	Rows    [][]typedCell `json:"rows"`
}
type typedImage struct {
	CommitID      string          `json:"commit_id"`
	ReadOnlyScope bool            `json:"read_only_scope"`
	Tables        []typedTable    `json:"tables"`
	SchemaProfile json.RawMessage `json:"schema_profile,omitempty"`
}
type typedWorking struct {
	Format          string       `json:"format"`
	Database        string       `json:"database"`
	HeadCommitID    string       `json:"head_commit_id"`
	WorkingRootHash string       `json:"working_root_hash"`
	ReadOnlyScope   bool         `json:"read_only_scope"`
	Complete        bool         `json:"complete"`
	Tables          []typedTable `json:"tables"`
}
type typedEvidence struct {
	Format          string          `json:"format"`
	ArchiveSHA256   string          `json:"archive_sha256"`
	StoreID         string          `json:"store_id"`
	Projects        []string        `json:"projects"`
	SQLDefaults     string          `json:"sql_defaults"`
	Working         typedWorking    `json:"working"`
	Images          []typedImage    `json:"images"`
	HistoryCoverage json.RawMessage `json:"history_coverage,omitempty"`
}

type typedBinding struct {
	Case             string `json:"case"`
	Role             string `json:"role"`
	Workspace        string `json:"workspace"`
	Database         string `json:"database"`
	StoreID          string `json:"store_id"`
	SourceSHA256     string `json:"source_sha256"`
	LiveSHA256       string `json:"live_sha256"`
	ImporterSHA256   string `json:"importer_sha256"`
	ImporterBuildRun string `json:"importer_build_run"`
	ReaderCommit     string `json:"reader_commit"`
	ReaderTree       string `json:"reader_tree"`
	ReaderSHA256     string `json:"reader_sha256"`
	ReaderBuildRun   string `json:"reader_build_run"`
}
type typedManifest struct {
	Binding        typedBinding  `json:"binding"`
	EvidenceSHA256 string        `json:"evidence_sha256"`
	Evidence       typedEvidence `json:"evidence"`
}
type typedAssessment struct {
	Format         string         `json:"format"`
	Case           string         `json:"case"`
	Status         string         `json:"status"`
	Gaps           []string       `json:"gaps"`
	Differences    []string       `json:"differences"`
	EqualityCredit int            `json:"equality_credit"`
	Observed       *typedManifest `json:"observed,omitempty"`
}

const typedReaderCommit = "e25cf419c9bc516fa3dd91394ee93a846255d7ba"
const typedReaderTree = "999b9ba07809893e41f59646170a6323c3c04312"

// Reject duplicate keys before struct decoding. In particular a later source
// identity, kind or value must never overwrite an earlier occurrence.
func strictTypedJSON(data []byte, out any) error {
	if len(data) > 128<<20 {
		return fmt.Errorf("typed evidence size limit")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var visit func(int) error
	visit = func(depth int) error {
		if depth > 64 {
			return fmt.Errorf("typed evidence nesting limit")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					key, err := d.Token()
					if err != nil {
						return err
					}
					name, ok := key.(string)
					if !ok || seen[name] {
						return fmt.Errorf("duplicate typed evidence key")
					}
					seen[name] = true
					if err := visit(depth + 1); err != nil {
						return err
					}
				}
			case '[':
				for d.More() {
					if err := visit(depth + 1); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("invalid typed evidence delimiter")
			}
			_, err = d.Token()
			return err
		}
		return nil
	}
	if err := visit(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing typed evidence")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	return d.Decode(out)
}

func (c *typedCell) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return fmt.Errorf("typed cell object required")
	}
	for key := range raw {
		if key != "kind" && key != "value" {
			return fmt.Errorf("unsupported typed cell field")
		}
	}
	if len(raw["kind"]) == 0 || raw["kind"][0] != '"' || json.Unmarshal(raw["kind"], &c.Kind) != nil {
		return fmt.Errorf("typed cell kind required")
	}
	c.Value = ""
	if value, ok := raw["value"]; ok {
		if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &c.Value) != nil || c.Kind == "sql-null" {
			return fmt.Errorf("invalid typed cell value")
		}
	}
	switch c.Kind {
	case "sql-null", "string":
		return nil
	case "bytes", "string-bytes":
		value, err := base64.StdEncoding.DecodeString(c.Value)
		if err == nil && base64.StdEncoding.EncodeToString(value) == c.Value {
			return nil
		}
	case "bool":
		if c.Value == "true" || c.Value == "false" {
			return nil
		}
	case "int8", "int16", "int32", "int64", "int":
		bits := 64
		if c.Kind != "int" {
			bits, _ = strconv.Atoi(strings.TrimPrefix(c.Kind, "int"))
		}
		value, err := strconv.ParseInt(c.Value, 10, bits)
		if err == nil && strconv.FormatInt(value, 10) == c.Value {
			return nil
		}
	case "uint8", "uint16", "uint32", "uint64", "uint":
		bits := 64
		if c.Kind != "uint" {
			bits, _ = strconv.Atoi(strings.TrimPrefix(c.Kind, "uint"))
		}
		value, err := strconv.ParseUint(c.Value, 10, bits)
		if err == nil && strconv.FormatUint(value, 10) == c.Value {
			return nil
		}
	case "float32-bits", "float64-bits":
		bits := 64
		if c.Kind == "float32-bits" {
			bits = 32
		}
		value, err := strconv.ParseUint(c.Value, 16, bits)
		if err == nil && strconv.FormatUint(value, 16) == c.Value {
			return nil
		}
	case "time":
		value, err := time.Parse(time.RFC3339Nano, c.Value)
		if err == nil && value.Format(time.RFC3339Nano) == c.Value {
			return nil
		}
	}
	return fmt.Errorf("unsupported or noncanonical typed cell")
}

func adaptTypedManifest(data []byte, binding typedBinding, archiveSHA, head string, projects []string) (*typedManifest, error) {
	var evidence typedEvidence
	if err := strictTypedJSON(data, &evidence); err != nil {
		return nil, err
	}
	if binding.Case == "" || binding.Role != "observed-target" && binding.Role != "intended-source" || binding.Workspace == "" || binding.Database == "" || binding.StoreID == "" || len(binding.SourceSHA256) != 64 || len(binding.LiveSHA256) != 64 || len(binding.ImporterSHA256) != 64 || binding.ImporterBuildRun == "" || binding.ReaderCommit != typedReaderCommit || binding.ReaderTree != typedReaderTree || len(binding.ReaderSHA256) != 64 || binding.ReaderBuildRun == "" {
		return nil, fmt.Errorf("typed input provenance missing or unsupported")
	}
	if evidence.Format != "beads-physical-evidence/1" || evidence.SQLDefaults != "not-captured" || evidence.ArchiveSHA256 != archiveSHA || len(archiveSHA) != 64 || evidence.StoreID != binding.StoreID || len(projects) == 0 || !reflect.DeepEqual(evidence.Projects, projects) || evidence.Working.Format != "beads-physical-working-root/1" || evidence.Working.Database != binding.Database || evidence.Working.HeadCommitID != head || head == "" || evidence.Working.WorkingRootHash == "" || !evidence.Working.ReadOnlyScope || !evidence.Working.Complete {
		return nil, fmt.Errorf("typed input source identity or completeness mismatch")
	}
	if err := validateTypedTables(evidence.Working.Tables); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, image := range evidence.Images {
		if image.CommitID == "" || seen[image.CommitID] || !image.ReadOnlyScope {
			return nil, fmt.Errorf("missing or duplicate typed immutable image")
		}
		seen[image.CommitID] = true
		if err := validateTypedTables(image.Tables); err != nil {
			return nil, err
		}
	}
	if !seen[head] {
		return nil, fmt.Errorf("typed head image missing")
	}
	return &typedManifest{Binding: binding, EvidenceSHA256: hash(data), Evidence: evidence}, nil
}

func validateTypedTables(tables []typedTable) error {
	if len(tables) == 0 {
		return fmt.Errorf("typed table census missing")
	}
	seen := map[string]bool{}
	for _, table := range tables {
		if table.Name == "" || seen[table.Name] || len(table.Columns) == 0 || table.Rows == nil {
			return fmt.Errorf("missing or duplicate typed table")
		}
		seen[table.Name] = true
		columns := map[string]bool{}
		for _, column := range table.Columns {
			if column.Name == "" || column.SQLType == "" || columns[column.Name] {
				return fmt.Errorf("missing or duplicate typed column")
			}
			columns[column.Name] = true
		}
		for _, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return fmt.Errorf("typed row width mismatch")
			}
		}
	}
	return nil
}

type typedArchiveTable struct {
	Columns []string                     `json:"columns"`
	Rows    []map[string]json.RawMessage `json:"rows"`
}
type typedArchiveHistory struct {
	Format   string                       `json:"format"`
	StoreID  string                       `json:"store_id"`
	Projects []string                     `json:"projects"`
	Head     string                       `json:"head"`
	Current  map[string]typedArchiveTable `json:"current"`
	Images   []struct {
		CommitID string                       `json:"commit_id"`
		Tables   map[string]typedArchiveTable `json:"tables"`
	} `json:"images"`
}

// This is coverage of the reader's own validated companion, not admission of
// that companion through the older normalized consumer. Only the physical
// evidence supplies SQL types and cell tags. The existing consumer/oracle
// verdict remains an independent prerequisite for any equality credit.
func bindTypedArchive(archive []byte, manifest *typedManifest) error {
	if len(archive) > 128<<20 || hash(archive) != manifest.Evidence.ArchiveSHA256 {
		return fmt.Errorf("typed companion archive mismatch")
	}
	reader := tar.NewReader(bytes.NewReader(archive))
	var history typedArchiveHistory
	found := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Name != "history/native-tables.json" {
			continue
		}
		if found || header.Size > 128<<20 || header.Typeflag != tar.TypeReg {
			return fmt.Errorf("duplicate or unsupported typed companion history")
		}
		found = true
		data, err := io.ReadAll(io.LimitReader(reader, (128<<20)+1))
		if err != nil || len(data) > 128<<20 {
			return fmt.Errorf("typed companion history size limit")
		}
		// Native schema profiles evolve in the unchanged reader. Preserve
		// its complete object while selecting only the census common to the
		// port; never substitute normalized values for physical cells.
		var object map[string]json.RawMessage
		if err := strictTypedJSON(data, &object); err != nil {
			return err
		}
		if err := json.Unmarshal(data, &history); err != nil {
			return err
		}
	}
	if !found || history.Format != "beads-native-tables/1" || history.StoreID != manifest.Evidence.StoreID || history.Head != manifest.Evidence.Working.HeadCommitID || !reflect.DeepEqual(history.Projects, manifest.Evidence.Projects) {
		return fmt.Errorf("typed companion source mismatch")
	}
	coverage := func(tables []typedTable, census map[string]typedArchiveTable) error {
		if len(tables) != len(census) {
			return fmt.Errorf("typed family coverage mismatch")
		}
		for _, table := range tables {
			raw, ok := census[table.Name]
			if !ok || len(raw.Rows) != len(table.Rows) || len(raw.Columns) != len(table.Columns) || raw.Rows == nil {
				return fmt.Errorf("typed row or column census mismatch")
			}
			for i, column := range table.Columns {
				if raw.Columns[i] != column.Name {
					return fmt.Errorf("typed column order mismatch")
				}
			}
			for _, row := range raw.Rows {
				if len(row) != len(raw.Columns) {
					return fmt.Errorf("typed companion row width mismatch")
				}
				for _, name := range raw.Columns {
					if _, ok := row[name]; !ok {
						return fmt.Errorf("typed companion column absent")
					}
				}
			}
		}
		return nil
	}
	if err := coverage(manifest.Evidence.Working.Tables, history.Current); err != nil {
		return err
	}
	if len(history.Images) != len(manifest.Evidence.Images) {
		return fmt.Errorf("typed immutable image census mismatch")
	}
	images := map[string]typedImage{}
	for _, image := range manifest.Evidence.Images {
		images[image.CommitID] = image
	}
	seen := map[string]bool{}
	for _, raw := range history.Images {
		image, ok := images[raw.CommitID]
		if !ok || seen[raw.CommitID] {
			return fmt.Errorf("typed immutable image identity mismatch")
		}
		seen[raw.CommitID] = true
		if err := coverage(image.Tables, raw.Tables); err != nil {
			return err
		}
	}
	return nil
}

// SQL type, column presence/order and exact tagged cells participate in the
// comparison. Row order does not; multiplicity does. No normalized defaults
// or metadata values fill an absent native column or cell.
func typedTableDifferences(expected, observed []typedTable, scope string) []string {
	left, right := map[string]typedTable{}, map[string]typedTable{}
	for _, table := range expected {
		left[table.Name] = table
	}
	for _, table := range observed {
		right[table.Name] = table
	}
	all := map[string]bool{}
	for name := range left {
		all[name] = true
	}
	for name := range right {
		all[name] = true
	}
	differences := []string{}
	for name := range all {
		a, aOK := left[name]
		b, bOK := right[name]
		if !aOK || !bOK {
			differences = append(differences, scope+"/"+name+"/presence")
			continue
		}
		if !reflect.DeepEqual(a.Columns, b.Columns) {
			differences = append(differences, scope+"/"+name+"/columns-and-sql-types")
		}
		rows := func(table typedTable) []string {
			values := []string{}
			for _, row := range table.Rows {
				data, _ := json.Marshal(row)
				values = append(values, string(data))
			}
			sort.Strings(values)
			return values
		}
		if !reflect.DeepEqual(rows(a), rows(b)) {
			differences = append(differences, scope+"/"+name+"/tagged-cells-and-row-multiplicity")
		}
	}
	sort.Strings(differences)
	return differences
}

func compareTypedManifests(caseName string, expected, observed *typedManifest, nativeAccepted bool) typedAssessment {
	result := typedAssessment{Format: "plug-public-typed-comparison/1", Case: caseName, Status: "GAP", Gaps: []string{}, Differences: []string{}, Observed: observed}
	if observed == nil {
		result.Gaps = append(result.Gaps, "observed_target_typed_input_missing")
	}
	if expected == nil {
		result.Gaps = append(result.Gaps, "intended_source_typed_input_not_supplied")
	}
	if !nativeAccepted {
		result.Gaps = append(result.Gaps, "existing_native_raw_or_metadata_oracle_not_equal")
	}
	if len(result.Gaps) != 0 {
		return result
	}
	if expected.Binding.Role != "intended-source" || observed.Binding.Role != "observed-target" || expected.Binding.Case != caseName || observed.Binding.Case != caseName || expected.Binding.SourceSHA256 != observed.Binding.SourceSHA256 || expected.Binding.LiveSHA256 != observed.Binding.LiveSHA256 || expected.Binding.Workspace == observed.Binding.Workspace || expected.Binding.StoreID == observed.Binding.StoreID || expected.EvidenceSHA256 == observed.EvidenceSHA256 {
		result.Gaps = append(result.Gaps, "wrong_source_or_self_comparison")
		return result
	}
	result.Differences = typedTableDifferences(expected.Evidence.Working.Tables, observed.Evidence.Working.Tables, "working")
	// Immutable image identity and complete history are compared separately;
	// a post-import commit cannot stand in for its original source commit.
	if !reflect.DeepEqual(expected.Evidence.Images, observed.Evidence.Images) {
		result.Differences = append(result.Differences, "immutable-images")
	}
	result.Status = "FAIL"
	if len(result.Differences) == 0 {
		result.Status, result.EqualityCredit = "PASS", 1
	}
	return result
}
