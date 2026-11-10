package converterroundtrip

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// These four members are original immutable producer outputs from
// 072a1a8f3ece496e359e213519357c924fcb203d, run 42759960008. The enclosing
// container changes no member bytes and confers no producer runtime verdict.
func nativeCommitFixtureMembers(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-multiversion-072a1a8f.tar")
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(raw) != "72df289c1e94535815764be53bc9f01530e683d6c7480ee06fc743e7a69a230b" {
		t.Fatal("original multi-version fixture container changed")
	}
	members := tarMembers(t, raw)
	want := map[string]string{
		"public-multiversion-v66.tar":                      "a1dc846d494397cce2b6b65038d2cb437449743026b92664e5152f297a8006d7",
		"public-multiversion-v66-physical-evidence.json":   "5e55747e01c9d95379a93432e7707655cc1f20081fc115e0eeed1192262a682d",
		"public-multiversion-v66-facts.json":               "a8e9dfdd597e2e66a1efccdf2e11395aa9dc2969eeadfbeb78e7891ec2b8da94",
		"public-multiversion-unsupported-diagnostic.jsonl": "586766e40eceab2b75c2505355ff85f3320643ffee3b2abc43da3d6bbae4b5fd",
	}
	if len(members) != len(want) {
		t.Fatal("original multi-version fixture member census changed")
	}
	for name, hash := range want {
		if hashBytes(members[name]) != hash {
			t.Fatal("original multi-version artifact changed", name)
		}
	}
	return members
}

func actualMultiVersionArchive(t *testing.T) (*dumpformat.Archive, []byte) {
	t.Helper()
	members := nativeCommitFixtureMembers(t)
	raw := members["public-multiversion-v66.tar"]
	return checkedNativeMultiVersionArchive(t, raw, members["public-multiversion-v66-facts.json"])
}

func checkedNativeMultiVersionArchive(t *testing.T, raw, factsRaw []byte) (*dumpformat.Archive, []byte) {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal("original populated multi-version archive", err)
	}
	var facts struct {
		ArchiveSHA256             string            `json:"archive_sha256"`
		Checkpoints               map[string]string `json:"checkpoints"`
		Commits                   int               `json:"commits"`
		Images                    int               `json:"images"`
		Gaps                      int               `json:"gaps"`
		CursorAbsentInitialImages int               `json:"cursor_absent_initial_images"`
		InputUnchangedAfterReopen bool              `json:"input_unchanged_after_reopen"`
		SchemaVersions            map[string]int    `json:"schema_versions"`
	}
	decode(t, factsRaw, &facts)
	scopes := archive.NativeSourceHistories()
	if archive.Manifest().MainVersion != 66 || archive.Manifest().IgnoredVersion != 26 || len(scopes) != 1 || facts.ArchiveSHA256 != archive.SHA256() || facts.Commits != 20 || facts.Images != 20 || facts.Gaps != 0 || facts.CursorAbsentInitialImages != 2 || !facts.InputUnchangedAfterReopen {
		t.Fatal("original current profile/history/facts binding differs")
	}
	scope := scopes[0]
	if len(scope.Current) != 30 || len(scope.Commits) != 20 || len(scope.Images) != 20 || !scope.Complete || len(scope.Gaps) != 0 {
		t.Fatal("original current family or historical image census differs")
	}
	versions := map[string]int{}
	checkpoints := map[string]uint64{}
	nullEmpty, nullIgnore, tables, rows, currentRows := 0, 0, 0, 0, 0
	for _, image := range scope.Images {
		if image.SchemaProfile == nil {
			t.Fatal("original image lacks its own observed schema profile")
		}
		p := image.SchemaProfile
		if p.MainVersion == nil {
			if p.IgnoredVersion != nil {
				t.Fatal("original initial root has an ignored cursor")
			}
			switch len(image.Tables) {
			case 0:
				nullEmpty++
			case 1:
				ignore, present := image.Tables["dolt_ignore"]
				if !present || !reflect.DeepEqual(ignore.Columns, []string{"ignored", "pattern"}) {
					t.Fatal("original cursorless root is not the exact ignore-only root")
				}
				nullIgnore++
			default:
				t.Fatal("original cursorless root has unrelated tables")
			}
		} else {
			versions[strconv.FormatUint(*p.MainVersion, 10)]++
			checkpoints[image.CommitID] = *p.MainVersion
		}
		tables += len(image.Tables)
		for _, table := range image.Tables {
			rows += len(table.Rows)
		}
	}
	for _, table := range scope.Current {
		currentRows += len(table.Rows)
	}
	if nullEmpty != 1 || nullIgnore != 1 || tables != 372 || rows != 1351 || currentRows != 109 || !reflect.DeepEqual(versions, facts.SchemaVersions) {
		t.Fatal("original per-commit schema, typed row or initial-root census differs")
	}
	if len(facts.Checkpoints) != 11 {
		t.Fatal("original checkpoint census differs")
	}
	for _, version := range []uint64{39, 40, 41, 43, 54, 55, 61, 62, 63, 64, 66} {
		id, present := facts.Checkpoints[strconv.FormatUint(version, 10)]
		if !present || checkpoints[id] != version {
			t.Fatal("original named checkpoint lacks its own immutable schema", version)
		}
	}
	return archive, raw
}

func TestActualNativeMultiVersionPhysicalInput(t *testing.T) {
	archive, _ := actualMultiVersionArchive(t)
	members := nativeCommitFixtureMembers(t)
	assertNativeCommitPhysicalInput(t, archive, members["public-multiversion-v66-physical-evidence.json"])
}

type nativeCommitPhysicalCell struct {
	Kind  string `json:"kind"`
	Value string `json:"value,omitempty"`
}

type nativeCommitPhysicalTable struct {
	Name    string `json:"name"`
	Columns []struct {
		Name    string `json:"name"`
		SQLType string `json:"sql_type"`
	} `json:"columns"`
	Rows [][]nativeCommitPhysicalCell `json:"rows"`
}

// Independently compare the original typed sidecar with the archive's
// documented timestamp/JSON/binary representation. SQL type tags remain
// physical evidence; this comparison does not invent target SQL schemas.
func nativeCommitPhysicalTableValue(t *testing.T, table nativeCommitPhysicalTable) dumpformat.NativeTable {
	t.Helper()
	value := dumpformat.NativeTable{Columns: []string{}, Rows: []dumpformat.Row{}}
	seen := map[string]bool{}
	for _, column := range table.Columns {
		if column.Name == "" || column.SQLType == "" || seen[column.Name] {
			t.Fatal("physical table has missing or duplicate typed columns")
		}
		seen[column.Name] = true
		value.Columns = append(value.Columns, column.Name)
	}
	sort.Strings(value.Columns)
	for _, cells := range table.Rows {
		if len(cells) != len(table.Columns) {
			t.Fatal("typed physical row width differs from its schema")
		}
		row := dumpformat.Row{}
		for i, cell := range cells {
			column := table.Columns[i]
			var got any
			switch cell.Kind {
			case "sql-null":
				if cell.Value != "" {
					t.Fatal("SQL NULL acquired a value")
				}
			case "string":
				got = cell.Value
			case "bytes", "string-bytes":
				raw, err := base64.StdEncoding.DecodeString(cell.Value)
				if err != nil {
					t.Fatal(err)
				}
				kind := strings.ToUpper(column.SQLType)
				if cell.Kind == "bytes" && (strings.Contains(kind, "BLOB") || strings.Contains(kind, "BINARY")) {
					got = map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)}
				} else {
					got = string(raw)
				}
			case "bool":
				parsed, err := strconv.ParseBool(cell.Value)
				if err != nil || strconv.FormatBool(parsed) != cell.Value {
					t.Fatal("invalid physical boolean", err)
				}
				got = parsed
			case "int", "int8", "int16", "int32", "int64":
				parsed, err := strconv.ParseInt(cell.Value, 10, 64)
				if err != nil || strconv.FormatInt(parsed, 10) != cell.Value {
					t.Fatal("invalid physical signed integer", err)
				}
				got = json.Number(cell.Value)
			case "uint", "uint8", "uint16", "uint32", "uint64":
				parsed, err := strconv.ParseUint(cell.Value, 10, 64)
				if err != nil || strconv.FormatUint(parsed, 10) != cell.Value {
					t.Fatal("invalid physical unsigned integer", err)
				}
				got = json.Number(cell.Value)
			case "float32-bits", "float64-bits":
				width := 64
				if cell.Kind == "float32-bits" {
					width = 32
				}
				bits, err := strconv.ParseUint(cell.Value, 16, width)
				if err != nil {
					t.Fatal(err)
				}
				f := math.Float64frombits(bits)
				if width == 32 {
					f = float64(math.Float32frombits(uint32(bits)))
				}
				if math.IsNaN(f) || math.IsInf(f, 0) {
					t.Fatal("non-finite physical number cannot be normalized as JSON")
				}
				got = f
				if width == 32 {
					got = float32(f)
				}
			case "time":
				when, err := time.Parse(time.RFC3339Nano, cell.Value)
				if err != nil {
					t.Fatal(err)
				}
				got = when.UTC().Format(time.RFC3339Nano)
			default:
				t.Fatal("unsupported typed physical cell", cell.Kind)
			}
			if text, ok := got.(string); ok && strings.EqualFold(column.SQLType, "JSON") {
				if err := decodeNativeSchemaFixture([]byte(text), &got); err != nil {
					t.Fatal("invalid physical JSON text", err)
				}
			}
			row[column.Name] = got
		}
		value.Rows = append(value.Rows, row)
	}
	sort.Slice(value.Rows, func(i, j int) bool {
		return bytes.Compare(encode(t, value.Rows[i]), encode(t, value.Rows[j])) < 0
	})
	return value
}

func assertNativeCommitPhysicalInput(t *testing.T, archive *dumpformat.Archive, raw []byte) {
	t.Helper()
	versions := assertNativeHistoryPhysicalInput(t, archive, raw, true)
	assertNativeCommitTransitions(t, versions)
	scope := archive.NativeSourceHistories()[0]
	t.Logf("MULTIVERSION_ORIGINAL_TYPED_INPUT source_count=1 images=%d current_families=%d checkpoints=%s all_current_and_image_cells=true sql_defaults=not-captured", len(scope.Images), len(scope.Current), fmt.Sprint([]int{39, 40, 41, 43, 54, 55, 61, 62, 63, 64, 66}))
}

// The complete fixture keeps its explicit completeness and migration oracles.
// A bounded fixture must independently match the whole coverage descriptor in
// the archive and the original physical sidecar, as well as every retained cell.
func assertNativeHistoryPhysicalInput(t *testing.T, archive *dumpformat.Archive, raw []byte, complete bool) map[uint64][]map[string]dumpformat.NativeTable {
	t.Helper()
	var evidence struct {
		Format          string                            `json:"format"`
		ArchiveSHA256   string                            `json:"archive_sha256"`
		StoreID         string                            `json:"store_id"`
		Projects        []string                          `json:"projects"`
		SQLDefaults     string                            `json:"sql_defaults"`
		HistoryCoverage *dumpformat.NativeHistoryCoverage `json:"history_coverage,omitempty"`
		Working         struct {
			HeadCommitID    string                      `json:"head_commit_id"`
			WorkingRootHash string                      `json:"working_root_hash"`
			ReadOnlyScope   bool                        `json:"read_only_scope"`
			Complete        bool                        `json:"complete"`
			Tables          []nativeCommitPhysicalTable `json:"tables"`
		} `json:"working"`
		Images []struct {
			CommitID      string                                `json:"commit_id"`
			ReadOnlyScope bool                                  `json:"read_only_scope"`
			Tables        []nativeCommitPhysicalTable           `json:"tables"`
			SchemaProfile *dumpformat.NativeCommitSchemaProfile `json:"schema_profile"`
		} `json:"images"`
	}
	if err := decodeNativeSchemaFixture(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	scopes := archive.NativeSourceHistories()
	if len(scopes) != 1 {
		t.Fatal("original genuine fixture must have exactly one native source")
	}
	scope := scopes[0]
	if evidence.Format != "beads-physical-evidence/1" || evidence.ArchiveSHA256 != archive.SHA256() || evidence.StoreID != scope.StoreID || !reflect.DeepEqual(evidence.Projects, scope.Projects) || evidence.SQLDefaults != "not-captured" || !evidence.Working.ReadOnlyScope || !evidence.Working.Complete || evidence.Working.WorkingRootHash == "" || evidence.Working.HeadCommitID != scope.Head || len(evidence.Images) != len(scope.Images) || scope.Complete != complete || !reflect.DeepEqual(evidence.HistoryCoverage, scope.HistoryCoverage) || len(scope.Gaps) != 0 {
		t.Fatal("genuine typed sidecar/source/current/history binding differs")
	}
	working := map[string]dumpformat.NativeTable{}
	for _, table := range evidence.Working.Tables {
		if _, duplicate := working[table.Name]; duplicate {
			t.Fatal("duplicate physical working table")
		}
		working[table.Name] = nativeCommitPhysicalTableValue(t, table)
	}
	for name, value := range scope.Current {
		if !bytes.Equal(encode(t, value), encode(t, working[name])) {
			t.Fatal("current family/cells differ from original typed working root", name)
		}
		delete(working, name)
	}
	if len(working) != 3 || !bytes.Equal(encode(t, working["dolt_ignore"]), encode(t, scope.Ignore)) {
		t.Fatal("physical current/catalog census differs")
	}
	for _, name := range []string{"dolt_ignore", "dolt_schemas", "dolt_nonlocal_tables"} {
		if _, present := working[name]; !present {
			t.Fatal("original physical catalog missing", name)
		}
	}
	versions := map[uint64][]map[string]dumpformat.NativeTable{}
	for i, image := range scope.Images {
		typed := evidence.Images[i]
		if typed.CommitID != image.CommitID || !typed.ReadOnlyScope || image.SchemaProfile == nil || !reflect.DeepEqual(typed.SchemaProfile, image.SchemaProfile) {
			t.Fatal("per-image schema/cursor facts differ from the same physical root")
		}
		tables := map[string]dumpformat.NativeTable{}
		for _, table := range typed.Tables {
			if _, duplicate := tables[table.Name]; duplicate {
				t.Fatal("duplicate physical image table")
			}
			tables[table.Name] = nativeCommitPhysicalTableValue(t, table)
		}
		if !bytes.Equal(encode(t, tables), encode(t, image.Tables)) {
			t.Fatal("historical table/column/cell/multiplicity differs from typed physical image", image.Sequence)
		}
		if image.SchemaProfile.MainVersion != nil {
			v := *image.SchemaProfile.MainVersion
			versions[v] = append(versions[v], tables)
		}
	}
	return versions
}

func assertNativeCommitTransitions(t *testing.T, versions map[uint64][]map[string]dumpformat.NativeTable) {
	t.Helper()
	for _, version := range []uint64{39, 40, 41, 43, 54, 55, 61, 62, 63, 64, 66} {
		if len(versions[version]) == 0 {
			t.Fatal("original multi-version checkpoint absent", version)
		}
	}
	hasColumn := func(table dumpformat.NativeTable, column string) bool {
		for _, name := range table.Columns {
			if name == column {
				return true
			}
		}
		return false
	}
	transition := func(version uint64, accept func(map[string]dumpformat.NativeTable) bool) bool {
		for _, image := range versions[version] {
			if accept(image) {
				return true
			}
		}
		return false
	}
	if !transition(39, func(t map[string]dumpformat.NativeTable) bool {
		return len(t["dependencies"].Rows) != 0 && hasColumn(t["dependencies"], "depends_on_id")
	}) || !transition(43, func(t map[string]dumpformat.NativeTable) bool {
		return !hasColumn(t["dependencies"], "depends_on_id") && hasColumn(t["dependencies"], "depends_on_issue_id")
	}) || !transition(54, func(t map[string]dumpformat.NativeTable) bool { return hasColumn(t["issues"], "lease_expires_at") }) || !transition(55, func(t map[string]dumpformat.NativeTable) bool { return !hasColumn(t["issues"], "lease_expires_at") }) || !transition(61, func(t map[string]dumpformat.NativeTable) bool { return len(t["events"].Rows) != 0 }) || !transition(62, func(t map[string]dumpformat.NativeTable) bool { _, present := t["events"]; return !present }) {
		t.Fatal("genuine populated dependency reshape, dropped column or dropped event table missing")
	}
}
