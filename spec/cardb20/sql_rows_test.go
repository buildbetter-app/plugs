package cardb20

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Native SQL emits JSON numbers whose spelling is part of the physical-row
// observation. UseNumber retains it; token decoding also refuses duplicate
// fields instead of silently replacing an earlier authoritative cell.
func decodeNativeSQLRows(raw []byte) ([]map[string]any, error) {
	value, err := decodeNativeSQLDocument(raw)
	if err != nil {
		return nil, err
	}
	return nativeSQLRowsValue(value)
}

func decodeNativeSQLDocument(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := nativeSQLValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing SQL JSON: %v", err)
	}
	return value, nil
}

func nativeSQLRowsValue(value any) ([]map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("SQL rows must be an array or null")
	}
	rows := make([]map[string]any, len(array))
	for i, value := range array {
		row, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("SQL row %d is not an object", i)
		}
		rows[i] = row
	}
	return rows, nil
}

func nativeSQLValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("SQL JSON object field is not a string")
			}
			if _, exists := object[name]; exists {
				return nil, fmt.Errorf("duplicate SQL JSON field %q", name)
			}
			value, err := nativeSQLValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if close, err := decoder.Token(); err != nil || close != json.Delim('}') {
			return nil, fmt.Errorf("invalid SQL JSON object terminator: %v", err)
		}
		return object, nil
	case json.Delim('['):
		array := make([]any, 0)
		for decoder.More() {
			value, err := nativeSQLValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if close, err := decoder.Token(); err != nil || close != json.Delim(']') {
			return nil, fmt.Errorf("invalid SQL JSON array terminator: %v", err)
		}
		return array, nil
	default:
		if _, delimiter := token.(json.Delim); delimiter {
			return nil, fmt.Errorf("unexpected SQL JSON delimiter %v", token)
		}
		return token, nil
	}
}

func verifyNativeSQLRows(raw []byte, rows []map[string]any) error {
	expected, err := decodeNativeSQLRows(raw)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, rows) {
		return fmt.Errorf("derived SQL rows differ from authoritative cells or numeric spellings/types")
	}
	serialized, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	decoded, err := decodeNativeSQLRows(serialized)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, decoded) {
		return fmt.Errorf("serialized SQL rows differ from authoritative cells")
	}
	return nil
}

// Only this JSON scalar comparison bridges SQL's lossless json.Number with
// the unchanged CLI decoder's small float64 sequence values. Neither value
// is converted to float64, and numbers remain distinct from strings/null.
func nativeSQLJournalCellEqual(sql, cli any) bool {
	a, err := json.Marshal(sql)
	if err != nil {
		return false
	}
	b, err := json.Marshal(cli)
	return err == nil && bytes.Equal(a, b)
}

type nativeSQLImageConservation struct {
	Twin       string `json:"twin"`
	Checkpoint string `json:"checkpoint"`
	Family     string `json:"family"`
	RawSHA256  string `json:"raw_sha256"`
	RowsSHA256 string `json:"rows_sha256"`
	RowCount   int    `json:"row_count"`
	CellCount  int    `json:"cell_count"`
}

type nativeSQLFileConservation struct {
	Path   string                       `json:"path"`
	SHA256 string                       `json:"sha256"`
	Images []nativeSQLImageConservation `json:"images"`
}

func nativeSQLDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// Read the completed derivative, not an in-memory twin or a second rounded
// derivative. Its twin/checkpoint/family inventory must equal the independent
// original SQL stdout inventory; every saved hash and exact cell is checked
// against those bytes and the original command's stdout digest.
func verifyNativeSQLRecordingFile(path string, rawRoots map[string]string) (nativeSQLFileConservation, error) {
	report := nativeSQLFileConservation{Path: path}
	if len(rawRoots) == 0 {
		return report, fmt.Errorf("authoritative SQL roots are required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return report, err
	}
	report.SHA256 = nativeSQLDigest(data)
	document, err := decodeNativeSQLDocument(data)
	if err != nil {
		return report, err
	}
	var twins []any
	switch value := document.(type) {
	case map[string]any:
		twins = []any{value}
	case []any:
		twins = value
	default:
		return report, fmt.Errorf("recording is not a twin or twin array")
	}
	seen := map[string]bool{}
	families := []string{"events", "issues", "journal", "sequence"}
	for _, value := range twins {
		twin, ok := value.(map[string]any)
		if !ok {
			return report, fmt.Errorf("recording twin is not an object")
		}
		name, ok := twin["name"].(string)
		rawRoot, known := rawRoots[name]
		if !ok || !known || seen[name] {
			return report, fmt.Errorf("unknown or duplicate recording twin %q", name)
		}
		seen[name] = true
		entries, err := os.ReadDir(rawRoot)
		if err != nil {
			return report, err
		}
		originals := map[string]map[string]string{}
		for _, entry := range entries {
			for _, family := range families {
				checkpoint, found := strings.CutSuffix(entry.Name(), "-"+family+".stdout")
				if !found {
					continue
				}
				if checkpoint == "" || !entry.Type().IsRegular() {
					return report, fmt.Errorf("invalid original SQL file %q", entry.Name())
				}
				if originals[checkpoint] == nil {
					originals[checkpoint] = map[string]string{}
				}
				originals[checkpoint][family] = filepath.Join(rawRoot, entry.Name())
			}
		}
		images, ok := twin["current_images"].(map[string]any)
		if !ok || len(originals) == 0 || len(images) != len(originals) {
			return report, fmt.Errorf("twin %s checkpoint inventory differs from original SQL", name)
		}
		checkpoints := make([]string, 0, len(originals))
		for checkpoint := range originals {
			checkpoints = append(checkpoints, checkpoint)
		}
		sort.Strings(checkpoints)
		for _, checkpoint := range checkpoints {
			image, ok := images[checkpoint].(map[string]any)
			if !ok || len(originals[checkpoint]) != len(families) {
				return report, fmt.Errorf("twin %s checkpoint %s is incomplete", name, checkpoint)
			}
			rows, rowsOK := image["rows"].(map[string]any)
			hashes, hashesOK := image["raw_sha256"].(map[string]any)
			if !rowsOK || !hashesOK || len(rows) != len(families) || len(hashes) != len(families) {
				return report, fmt.Errorf("twin %s checkpoint %s family inventory differs", name, checkpoint)
			}
			for _, family := range families {
				rawPath := originals[checkpoint][family]
				raw, err := os.ReadFile(rawPath)
				if err != nil {
					return report, err
				}
				digest := nativeSQLDigest(raw)
				if hashes[family] != digest {
					return report, fmt.Errorf("%s/%s/%s raw SHA256 mismatch", name, checkpoint, family)
				}
				request, err := os.ReadFile(strings.TrimSuffix(rawPath, ".stdout") + ".request.json")
				if err != nil {
					return report, err
				}
				requestValue, err := decodeNativeSQLDocument(request)
				if err != nil {
					return report, err
				}
				command, ok := requestValue.(map[string]any)
				if !ok || command["stdout_sha256"] != digest || command["exit"] != json.Number("0") {
					return report, fmt.Errorf("%s/%s/%s original command digest/exit mismatch", name, checkpoint, family)
				}
				cells, exists := rows[family]
				if !exists {
					return report, fmt.Errorf("%s/%s/%s rows missing", name, checkpoint, family)
				}
				derived, err := nativeSQLRowsValue(cells)
				if err != nil {
					return report, err
				}
				if err := verifyNativeSQLRows(raw, derived); err != nil {
					return report, fmt.Errorf("%s/%s/%s: %w", name, checkpoint, family, err)
				}
				encoded, err := json.Marshal(derived)
				if err != nil {
					return report, err
				}
				count := 0
				for _, row := range derived {
					count += len(row)
				}
				report.Images = append(report.Images, nativeSQLImageConservation{name, checkpoint, family, digest, nativeSQLDigest(encoded), len(derived), count})
			}
		}
	}
	if len(seen) != len(rawRoots) {
		return report, fmt.Errorf("recorded twin inventory differs from original roots")
	}
	return report, nil
}

func TestB20SQLRowsPreserveExactNumericSpellings(t *testing.T) {
	for _, spelling := range []string{"9007199254740993", "1317802009495480065", "-9223372036854775808", "18446744073709551615", "0", "-0", "1.2300", "1e+20", "1E-400"} {
		t.Run(spelling, func(t *testing.T) {
			raw := []byte(`[{"row_lock":` + spelling + `,"nested":{"value":` + spelling + `},"array":[` + spelling + `]}]`)
			rows, err := decodeNativeSQLRows(raw)
			if err != nil {
				t.Fatal(err)
			}
			if rows[0]["row_lock"] != json.Number(spelling) || rows[0]["nested"].(map[string]any)["value"] != json.Number(spelling) || rows[0]["array"].([]any)[0] != json.Number(spelling) {
				t.Fatalf("numeric spelling/type changed: %#v", rows)
			}
			if err := verifyNativeSQLRows(raw, rows); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestB20SQLRowsPreserveCellTypesAndShape(t *testing.T) {
	raw := []byte(`[{"zero":0,"null":null,"string":"0","empty":"","bool":false,"array":[],"object":{}}]`)
	rows, err := decodeNativeSQLRows(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"zero": json.Number("0"), "null": nil, "string": "0", "empty": "", "bool": false, "array": []any{}, "object": map[string]any{}}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("cell types/shapes changed: %#v", rows)
	}
	for _, vector := range []struct {
		raw     string
		nilRows bool
	}{{"null", true}, {"[]", false}, {"[{}]", false}} {
		rows, err := decodeNativeSQLRows([]byte(vector.raw))
		if err != nil || (rows == nil) != vector.nilRows {
			t.Fatalf("root %s: rows=%#v err=%v", vector.raw, rows, err)
		}
		if err := verifyNativeSQLRows([]byte(vector.raw), rows); err != nil {
			t.Fatal(err)
		}
	}
}

func TestB20SQLRowsRefuseAmbiguousOrMalformed(t *testing.T) {
	vectors := map[string]string{
		"duplicate":         `[{"id":1,"id":2}]`,
		"escaped_duplicate": `[{"id":1,"\u0069d":2}]`,
		"nested_duplicate":  `[{"value":{"id":1,"id":2}}]`,
		"array_duplicate":   `[{"value":[{"id":1,"\u0069d":2}]}]`,
		"trailing_document": `[] []`, "trailing_token": `[] false`, "trailing_garbage": `[]x`,
		"truncated": `[{"id":1}`, "trailing_comma": `[{"id":1,}]`,
		"leading_zero": `[{"id":01}]`, "nan": `[{"id":NaN}]`, "infinity": `[{"id":Infinity}]`,
		"missing_exponent": `[{"id":1e}]`, "missing_fraction": `[{"id":1.}]`,
		"empty": ``, "object_root": `{}`, "string_root": `"[]"`, "scalar_row": `[1]`, "null_row": `[null]`,
	}
	for name, raw := range vectors {
		t.Run(name, func(t *testing.T) {
			if rows, err := decodeNativeSQLRows([]byte(raw)); err == nil {
				t.Fatalf("ambiguous/malformed SQL accepted: %#v", rows)
			}
		})
	}
}

func TestB20SQLRowsDetectDerivedCorruption(t *testing.T) {
	raw := []byte(`[{"id":1,"row_lock":1317802009495480065,"zero":0,"null":null},{"id":2}]`)
	mutations := map[string]func([]map[string]any){
		"rounded_float":    func(rows []map[string]any) { rows[0]["row_lock"] = float64(1317802009495480065) },
		"rounded_number":   func(rows []map[string]any) { rows[0]["row_lock"] = json.Number("1317802009495480000") },
		"number_as_string": func(rows []map[string]any) { rows[0]["row_lock"] = "1317802009495480065" },
		"zero_as_null":     func(rows []map[string]any) { rows[0]["zero"] = nil },
		"missing_null":     func(rows []map[string]any) { delete(rows[0], "null") },
		"extra_cell":       func(rows []map[string]any) { rows[0]["extra"] = nil },
		"row_order":        func(rows []map[string]any) { rows[0], rows[1] = rows[1], rows[0] },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			rows, err := decodeNativeSQLRows(raw)
			if err != nil {
				t.Fatal(err)
			}
			mutate(rows)
			if err := verifyNativeSQLRows(raw, rows); err == nil {
				t.Fatal("derived corruption accepted")
			}
		})
	}
}

func TestB20SQLJournalComparisonKeepsNumericTypes(t *testing.T) {
	for _, vector := range []struct {
		name     string
		sql, cli any
		equal    bool
	}{
		{"sequence", json.Number("3"), float64(3), true},
		{"operation", "create", "create", true},
		{"null", nil, nil, true},
		{"number_string", json.Number("3"), "3", false},
		{"large_rounded", json.Number("9007199254740993"), float64(9007199254740993), false},
		{"zero_null", json.Number("0"), nil, false},
		{"zero_bool", json.Number("0"), false, false},
	} {
		t.Run(vector.name, func(t *testing.T) {
			if got := nativeSQLJournalCellEqual(vector.sql, vector.cli); got != vector.equal {
				t.Fatalf("SQL/CLI comparison=%t want=%t", got, vector.equal)
			}
		})
	}
}

// Synthetic files exercise the verifier's corruption refusals only. Actual
// native qualification requires the manual recorder's fresh originals.
func b20SQLConservationControl(t *testing.T, aggregate bool) (string, map[string]string, any) {
	t.Helper()
	root := t.TempDir()
	roots := map[string]string{}
	twins := []any{}
	for _, name := range []string{"a", "b"} {
		dir := filepath.Join(root, "native-"+name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		roots[name] = dir
		images := map[string]any{}
		for _, checkpoint := range []string{"empty", "final"} {
			rows := map[string]any{}
			hashes := map[string]any{}
			for _, family := range []string{"events", "issues", "journal", "sequence"} {
				raw := []byte("null\n")
				if checkpoint == "final" {
					raw = []byte(`[{"row_lock":1317802009495480065,"zero":0,"nullable":null,"text":"0","nested":{"n":9007199254740993}}]` + "\n")
				}
				decoded, err := decodeNativeSQLRows(raw)
				if err != nil {
					t.Fatal(err)
				}
				rows[family] = decoded
				hashes[family] = nativeSQLDigest(raw)
				prefix := filepath.Join(dir, checkpoint+"-"+family)
				if err := os.WriteFile(prefix+".stdout", raw, 0600); err != nil {
					t.Fatal(err)
				}
				b20SQLWriteControlJSON(t, prefix+".request.json", map[string]any{"stdout_sha256": nativeSQLDigest(raw), "exit": 0})
			}
			images[checkpoint] = map[string]any{"rows": rows, "raw_sha256": hashes}
		}
		twins = append(twins, map[string]any{"name": name, "current_images": images})
	}
	path := filepath.Join(root, "native-twins.json")
	var document any = twins
	if !aggregate {
		path = filepath.Join(root, "recording.json")
		document = twins[0]
		delete(roots, "b")
	}
	b20SQLWriteControlJSON(t, path, document)
	return path, roots, document
}

func b20SQLWriteControlJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestB20SavedSQLRecordingsConserveAuthoritativeCells(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		t.Run(fmt.Sprintf("aggregate_%t", aggregate), func(t *testing.T) {
			path, roots, _ := b20SQLConservationControl(t, aggregate)
			report, err := verifyNativeSQLRecordingFile(path, roots)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Images) != len(roots)*8 || report.SHA256 != nativeSQLDigest(data) {
				t.Fatalf("incomplete file-bound conservation: %+v", report)
			}
		})
	}
}

func TestB20SavedSQLRecordingsRefuseCorruption(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		for _, mutation := range []string{"rounded_rows", "number_string", "zero_null", "missing_null", "extra_cell", "missing_family", "missing_checkpoint", "hash_forgery", "raw_corruption", "command_hash", "command_exit", "duplicate_field", "malformed", "missing_twin"} {
			t.Run(fmt.Sprintf("aggregate_%t/%s", aggregate, mutation), func(t *testing.T) {
				path, roots, document := b20SQLConservationControl(t, aggregate)
				twin := document
				if aggregate {
					twin = document.([]any)[0]
				}
				images := twin.(map[string]any)["current_images"].(map[string]any)
				image := images["final"].(map[string]any)
				rows := image["rows"].(map[string]any)
				cells := rows["issues"].([]map[string]any)[0]
				switch mutation {
				case "rounded_rows":
					cells["row_lock"] = json.Number("1317802009495480000")
				case "number_string":
					cells["row_lock"] = "1317802009495480065"
				case "zero_null":
					cells["zero"] = nil
				case "missing_null":
					delete(cells, "nullable")
				case "extra_cell":
					cells["extra"] = nil
				case "missing_family":
					delete(rows, "issues")
				case "missing_checkpoint":
					delete(images, "final")
				case "hash_forgery":
					image["raw_sha256"].(map[string]any)["issues"] = strings.Repeat("0", 64)
				case "raw_corruption":
					if err := os.WriteFile(filepath.Join(roots["a"], "final-issues.stdout"), []byte("[]"), 0600); err != nil {
						t.Fatal(err)
					}
				case "command_hash", "command_exit":
					exit, digest := 0, image["raw_sha256"].(map[string]any)["issues"]
					if mutation == "command_hash" {
						digest = "wrong"
					} else {
						exit = 3
					}
					b20SQLWriteControlJSON(t, filepath.Join(roots["a"], "final-issues.request.json"), map[string]any{"stdout_sha256": digest, "exit": exit})
				case "missing_twin":
					if aggregate {
						document = document.([]any)[1:]
					} else {
						twin.(map[string]any)["name"] = "missing"
					}
				}
				b20SQLWriteControlJSON(t, path, document)
				if mutation == "duplicate_field" || mutation == "malformed" {
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if mutation == "duplicate_field" {
						data = bytes.Replace(data, []byte(`"row_lock":1317802009495480065`), []byte(`"row_lock":1317802009495480065,"\u0072ow_lock":0`), 1)
					} else {
						data = data[:len(data)/2]
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := verifyNativeSQLRecordingFile(path, roots); err == nil {
					t.Fatal("corrupt saved recording accepted")
				}
			})
		}
	}
}

func TestB20MatchingRoundedSQLDerivativesAreRefused(t *testing.T) {
	path, roots, document := b20SQLConservationControl(t, true)
	twins := document.([]any)
	for _, value := range twins {
		images := value.(map[string]any)["current_images"].(map[string]any)
		rows := images["final"].(map[string]any)["rows"].(map[string]any)
		rows["issues"].([]map[string]any)[0]["row_lock"] = json.Number("1317802009495480000")
	}
	b20SQLWriteControlJSON(t, path, twins)
	individual := filepath.Join(roots["a"], "recording.json")
	b20SQLWriteControlJSON(t, individual, twins[0])
	if _, err := verifyNativeSQLRecordingFile(path, roots); err == nil {
		t.Fatal("rounded aggregate accepted against original SQL")
	}
	if _, err := verifyNativeSQLRecordingFile(individual, map[string]string{"a": roots["a"]}); err == nil {
		t.Fatal("equally rounded individual copy accepted against original SQL")
	}
}
