package plugdump

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type nativeDeltaState struct {
	Schema  NativeDeltaSchema                 `json:"schema"`
	Tables  map[string][]NativeDeltaRow       `json:"tables"`
	Digests map[string]NativeDeltaTableDigest `json:"digests"`
	Root    string                            `json:"root"`
	Native  map[string]NativeTable            `json:"-"`
}

func nativeDeltaPhysicalState(tables []physicalcapture.Table, policy map[string]string) (*nativeDeltaState, error) {
	normalized, err := physicalCaptureTables(tables)
	if err != nil {
		return nil, err
	}
	profile, err := ProfileNativeCommit(normalized)
	if err != nil {
		return nil, &NativeCommitFailure{SchemaVersion: profile.MainVersion, Cause: err}
	}
	return nativeDeltaPhysicalStateWithProfile(tables, profile, policy)
}

func nativeDeltaPhysicalStateWithProfile(tables []physicalcapture.Table, profile NativeCommitSchemaProfile, policy map[string]string) (*nativeDeltaState, error) {
	state := &nativeDeltaState{Schema: NativeDeltaSchema{Type: "schema", Profile: profile, Tables: []NativeDeltaTableSchema{}}, Tables: map[string][]NativeDeltaRow{}, Digests: map[string]NativeDeltaTableDigest{}, Native: map[string]NativeTable{}}
	ordered := append([]physicalcapture.Table{}, tables...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	for tableIndex, table := range ordered {
		if !nativeSQLName.MatchString(table.Name) || tableIndex > 0 && ordered[tableIndex-1].Name == table.Name {
			return nil, fmt.Errorf("invalid native delta table census")
		}
		schema := NativeDeltaTableSchema{Name: table.Name, Columns: []NativeDeltaColumn{}}
		columns := []string{}
		seenColumns := map[string]bool{}
		for _, column := range table.Columns {
			if !nativeSQLName.MatchString(column.Name) || seenColumns[column.Name] || column.DatabaseTypeName == "" {
				return nil, fmt.Errorf("invalid native delta column")
			}
			seenColumns[column.Name] = true
			columns = append(columns, column.Name)
			schema.Columns = append(schema.Columns, NativeDeltaColumn{Name: column.Name, SQLType: column.DatabaseTypeName, PrimaryKeyOrdinal: column.PrimaryKeyOrdinal})
		}
		// Validate key ordinals even for a physically empty table.
		dummy := make([]NativeDeltaCell, len(schema.Columns))
		for i := range dummy {
			dummy[i] = NativeDeltaCell{Kind: "string", Value: "key-metadata-check"}
		}
		if _, err := NativeDeltaPrimaryKey(schema, dummy); err != nil {
			return nil, err
		}
		evidence, err := physicalcapture.EvidenceTables([]physicalcapture.Table{table})
		if err != nil {
			return nil, err
		}
		rows := make([]NativeDeltaRow, 0, len(table.Rows))
		nativeRows := []Row{}
		multiplicity := map[string]uint64{}
		bagPayloads := map[string][]byte{}
		for rowIndex, values := range table.Rows {
			if len(values) != len(table.Columns) {
				return nil, fmt.Errorf("native delta row width differs")
			}
			original := Row{}
			for i, value := range values {
				converted, err := sqlValue(value, table.Columns[i].DatabaseTypeName)
				if err != nil {
					return nil, err
				}
				original[table.Columns[i].Name] = converted
			}
			checked, err := nativeTable(Table{Columns: columns, Rows: []Row{original}}, table.Name, policy)
			if err != nil {
				return nil, err
			}
			normal := checked.Rows[0]
			cells := make([]NativeDeltaCell, len(values))
			for i, value := range evidence[0].Rows[rowIndex] {
				cells[i] = NativeDeltaCell{Kind: value.Kind, Value: value.Value}
				if table.Columns[i].Name == "value" && (table.Name == "config" || table.Name == "metadata") && (secretKey(text(original, "key")) || policy[table.Name+"/"+text(original, "key")] == "secret") {
					cells[i] = NativeDeltaCell{Kind: "secret-reference", Value: table.Name + "/" + text(original, "key")}
				}
			}
			jsonRow, err := marshal(normal)
			if err != nil {
				return nil, err
			}
			payload := NativeDeltaRowPayload(cells, jsonRow)
			key, err := NativeDeltaPrimaryKey(schema, cells)
			if err != nil {
				return nil, err
			}
			if key == nil {
				bag := NativeDeltaBagKey(payload, 0)
				group := hex.EncodeToString(bag[4:36])
				if previous, exists := bagPayloads[group]; exists && !bytes.Equal(previous, payload) {
					return nil, fmt.Errorf("keyless content hash collision")
				}
				bagPayloads[group] = payload
				key = NativeDeltaBagKey(payload, multiplicity[group])
				multiplicity[group]++
			}
			rows = append(rows, NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(jsonRow)})
			nativeRows = append(nativeRows, normal)
		}
		sort.Slice(rows, func(i, j int) bool {
			a, _ := base64.StdEncoding.DecodeString(rows[i].Key)
			b, _ := base64.StdEncoding.DecodeString(rows[j].Key)
			return bytes.Compare(a, b) < 0
		})
		hash, err := NativeDeltaTableSHA256(schema, rows)
		if err != nil {
			return nil, err
		}
		state.Tables[table.Name] = rows
		state.Digests[table.Name] = NativeDeltaTableDigest{Rows: uint64(len(rows)), SHA256: hash}
		state.Schema.Tables = append(state.Schema.Tables, schema)
		canonical, err := nativeTable(Table{Columns: columns, Rows: nativeRows}, table.Name, policy)
		if err != nil {
			return nil, err
		}
		state.Native[table.Name] = canonical
	}
	state.Schema.SchemaID = NativeDeltaSchemaID(state.Schema)
	root, err := NativeDeltaRootSHA256(state.Digests)
	if err != nil {
		return nil, err
	}
	state.Root = root
	return state, nil
}
