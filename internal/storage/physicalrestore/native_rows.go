package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeTableBinding is the independently authenticated table census at the
// reader's exact revision. The caller supplies the source mapping and expected
// old-table commitment, never a census inferred from the records being checked.
// This port does not authenticate an archive or grant forward-import admission.
type NativeTableBinding struct {
	Source         dumpformat.SourceIdentity
	Table          string
	History        string
	IdentityKind   string
	BodySHA256     string
	PhysicalSHA256 string
	Refs           []driver.Ref
}

type nativeTableKey struct {
	Source         dumpformat.SourceIdentity
	Table, History string
}

type nativeRebuiltTable struct {
	binding NativeTableBinding
	meta    dumpformat.NativeRowMeta
	rows    []*dumpformat.NativeDecodedRow
	body    []byte
}

// NativeRowReadback owns complete, codec-validated groups from one immutable
// Reader snapshot. Its private state prevents a caller from replacing validated
// typed rows between reconstruction and preparation of the physical request.
type NativeRowReadback struct {
	tables []nativeRebuiltTable
}

// TableBodies returns detached old-table envelopes in the supplied census order.
// These bytes include source_identity/table/history/columns/rows, not just rows.
func (r NativeRowReadback) TableBodies() [][]byte {
	out := make([][]byte, len(r.tables))
	for i, table := range r.tables {
		out[i] = bytes.Clone(table.body)
	}
	return out
}

func nativeReadError(table, reason string) error {
	return &Error{Kind: "native-row-readback", Table: table, Column: "*", Row: -1, Cause: errors.New(reason)}
}

// ReadNativeRowTables enumerates unfiltered Query pages at one bound snapshot.
// Namespace selection precedes Kind validation; wrong-kind extras cannot hide.
// The caller owns the Reader lifetime and the original archive/root authority.
// Both native planes must be completely represented in the supplied census.
func ReadNativeRowTables(ctx context.Context, reader driver.Reader, census []NativeTableBinding, policy map[string]string) (NativeRowReadback, error) {
	if reader == nil {
		return NativeRowReadback{}, nativeReadError("", "fixed snapshot reader required")
	}
	view := reader.Snapshot()
	if view.At == nil || view.At.Validate() != nil || view.Incarnation == "" || !view.Branch.IsZero() {
		return NativeRowReadback{}, nativeReadError("", "bound incarnation/revision and branch zero required")
	}
	groups := make(map[nativeTableKey][]driver.Record, len(census))
	wanted := make(map[nativeTableKey]NativeTableBinding, len(census))
	expectedRefs := map[string]nativeTableKey{}
	for _, binding := range census {
		key := nativeTableKey{binding.Source, binding.Table, binding.History}
		digest, digestErr := hex.DecodeString(binding.BodySHA256)
		if _, duplicate := wanted[key]; duplicate || digestErr != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != binding.BodySHA256 || len(binding.Refs) == 0 {
			return NativeRowReadback{}, nativeReadError(binding.Table, "duplicate or unbound expected table")
		}
		if binding.IdentityKind == dumpformat.NativeRowD3Physical {
			digest, err := hex.DecodeString(binding.PhysicalSHA256)
			if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != binding.PhysicalSHA256 {
				return NativeRowReadback{}, nativeReadError(binding.Table, "original physical table commitment required")
			}
		} else if binding.IdentityKind != dumpformat.NativeRowV1ContentBag || binding.PhysicalSHA256 != "" {
			return NativeRowReadback{}, nativeReadError(binding.Table, "invalid expected identity/physical authority")
		}
		for _, ref := range binding.Refs {
			if _, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil || !ref.Branch.IsZero() {
				return NativeRowReadback{}, nativeReadError(binding.Table, "invalid or duplicate expected Ref")
			}
			if _, duplicate := expectedRefs[ref.ID]; duplicate {
				return NativeRowReadback{}, nativeReadError(binding.Table, "duplicate expected Ref")
			}
			expectedRefs[ref.ID] = key
		}
		wanted[key] = binding
	}
	var cursor *driver.Cursor
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return NativeRowReadback{}, err
		}
		page, err := reader.Query(ctx, driver.Query{Limit: 256, Cursor: cursor, AsOf: view.At})
		if err != nil {
			return NativeRowReadback{}, err
		}
		latest := reader.Snapshot()
		if page.AsOf.Validate() != nil || page.AsOf.String() != view.At.String() || latest.At == nil || latest.At.String() != view.At.String() || latest.Incarnation != view.Incarnation || !latest.Branch.IsZero() {
			return NativeRowReadback{}, nativeReadError("", "snapshot changed during enumeration")
		}
		for _, record := range page.Items {
			if !strings.HasPrefix(record.Ref.ID, driver.NativeTableHistoryRecordPrefix) && !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
				continue
			}
			expectedGroup, expected := expectedRefs[record.Ref.ID]
			if !expected || seen[record.Ref.ID] {
				return NativeRowReadback{}, nativeReadError("", "extra or duplicate native Ref across pages")
			}
			seen[record.Ref.ID] = true
			meta, row, err := dumpformat.DecodeNativeRowRecord(record)
			if err != nil {
				return NativeRowReadback{}, err
			}
			var key nativeTableKey
			if meta != nil {
				key = nativeTableKey{meta.Source, meta.Table, meta.History}
			} else {
				key = nativeTableKey{row.Source, row.Table, row.History}
			}
			if _, ok := wanted[key]; !ok || key != expectedGroup {
				return NativeRowReadback{}, nativeReadError(key.Table, "unexpected source/table/plane group")
			}
			groups[key] = append(groups[key], record)
		}
		if page.NextCursor == nil {
			break
		}
		next := page.NextCursor
		index, indexErr := next.Index.Uint64()
		prior := uint64(0)
		if cursor != nil {
			prior, _ = cursor.Index.Uint64()
		}
		filter := sha256.Sum256([]byte("null")) // Query.Filter is nil, never Kind-filtered.
		if len(page.Items) == 0 || indexErr != nil || index <= prior || index-prior != uint64(len(page.Items)) || next.Incarnation != view.Incarnation || next.Revision.String() != view.At.String() || next.Method != "query" || next.FilterDigest != hex.EncodeToString(filter[:]) || next.PrincipalDigest != "" || !next.OntologyRevision.IsZero() {
			return NativeRowReadback{}, nativeReadError("", "invalid or nonadvancing pagination cursor")
		}
		cursor = next
	}
	if len(seen) != len(expectedRefs) {
		return NativeRowReadback{}, nativeReadError("", "missing expected native Ref")
	}
	out := NativeRowReadback{tables: make([]nativeRebuiltTable, 0, len(census))}
	for _, binding := range census {
		if err := ctx.Err(); err != nil {
			return NativeRowReadback{}, err
		}
		key := nativeTableKey{binding.Source, binding.Table, binding.History}
		records := groups[key]
		table, meta, err := dumpformat.ReconstructNativeRowTable(binding.Source, binding.Table, binding.History, records, policy)
		if err != nil {
			return NativeRowReadback{}, err
		}
		body, err := dumpformat.NativeTableBodyBytes(binding.Source, binding.Table, binding.History, table)
		if err != nil {
			return NativeRowReadback{}, err
		}
		sum := sha256.Sum256(body)
		if meta.IdentityKind != binding.IdentityKind || hex.EncodeToString(sum[:]) != binding.BodySHA256 || binding.IdentityKind == dumpformat.NativeRowD3Physical && (meta.PhysicalTableSHA256 == nil || *meta.PhysicalTableSHA256 != binding.PhysicalSHA256) {
			return NativeRowReadback{}, nativeReadError(binding.Table, "independent expected identity/table bytes differ")
		}
		rebuilt := nativeRebuiltTable{binding: binding, meta: meta, body: body}
		for _, record := range records {
			_, row, err := dumpformat.DecodeNativeRowRecord(record)
			if err != nil {
				return NativeRowReadback{}, err
			}
			if row != nil {
				rebuilt.rows = append(rebuilt.rows, row)
			}
		}
		out.tables = append(out.tables, rebuilt)
		delete(groups, key)
	}
	if len(groups) != 0 {
		return NativeRowReadback{}, nativeReadError("", "leftover table groups")
	}
	return out, nil
}

// PrepareNativeRowUndo retains the writer's original two typed capture ports.
// V1 reconstruction succeeds independently; absent typed v1 cells refuse here.
// Reserved view/schema evidence is compared exactly and never sent for writes.
// Desired DDL, table census or SQL column/type changes refuse before Undo runs.
func PrepareNativeRowUndo(source SourceBinding, identity dumpformat.SourceIdentity, readback NativeRowReadback, report []byte) (UndoRequest, error) {
	if err := validateBinding(source); err != nil {
		return UndoRequest{}, err
	}
	if source.Working == nil {
		return UndoRequest{}, &Error{Kind: "source-completeness", Row: -1, Cause: errors.New("complete same-source typed working port required")}
	}
	versioned := map[string]bool{}
	for _, table := range source.Image.Tables {
		versioned[table.Name] = true
	}
	original := map[string]Table{}
	for _, table := range source.Working.Image.Tables {
		original[table.Name] = table
	}
	current := Manifest{Format: Format, HistoryMode: "snapshot", Tables: []Table{}}
	seen := map[string]bool{}
	for _, group := range readback.tables {
		if group.binding.Source != identity {
			return UndoRequest{}, nativeReadError(group.binding.Table, "physical undo source identity differs")
		}
		if group.binding.History == "retained-native-commit" {
			continue
		}
		name := group.meta.Table
		if seen[name] {
			return UndoRequest{}, nativeReadError(name, "multiple current classes for physical table")
		}
		seen[name] = true
		old, ok := original[name]
		if !ok || group.meta.IdentityKind != dumpformat.NativeRowD3Physical {
			return UndoRequest{}, &Error{Kind: "native-row-typed-source", Table: name, Column: "*", Row: -1, Cause: errors.New("existing table and original D3 typed cells required; normalized v1 is not physical write authority")}
		}
		wantHistory := dumpformat.FamilyUnversioned
		if versioned[name] {
			wantHistory = dumpformat.FamilyVersioned
		}
		if strings.HasPrefix(strings.ToLower(name), "dolt_") {
			wantHistory = dumpformat.NativeRowOpaqueCurrentHistory
		}
		if group.meta.History != wantHistory {
			return UndoRequest{}, nativeReadError(name, "current history class differs from original typed table family")
		}
		table := Table{Name: name, Columns: make([]Column, len(group.meta.PhysicalColumns)), Rows: make([][]Cell, len(group.rows))}
		for i, col := range group.meta.PhysicalColumns {
			table.Columns[i] = Column{Name: col.Name, SQLType: col.SQLType}
		}
		if !sameColumns(old.Columns, table.Columns) {
			return UndoRequest{}, &Error{Kind: "schema", Table: name, Column: "*", Row: -1, Cause: errors.New("destination exact SQL column order/types must remain unchanged")}
		}
		for ri, row := range group.rows {
			table.Rows[ri] = make([]Cell, len(row.Cells))
			for ci, cell := range row.Cells {
				converted, err := nativeUndoCell(cell)
				if err != nil {
					return UndoRequest{}, &Error{Kind: "cell-type", Table: name, Column: table.Columns[ci].Name, Row: ri, Cause: err}
				}
				table.Rows[ri][ci] = converted
			}
		}
		if strings.HasPrefix(strings.ToLower(name), "dolt_") {
			left, err := CanonicalPhysicalBytes(Manifest{Format: Format, HistoryMode: "snapshot", Tables: []Table{old}})
			if err != nil {
				return UndoRequest{}, err
			}
			right, err := CanonicalPhysicalBytes(Manifest{Format: Format, HistoryMode: "snapshot", Tables: []Table{table}})
			if err != nil || !bytes.Equal(left, right) {
				return UndoRequest{}, &Error{Kind: "reserved-schema", Table: name, Column: "*", Row: -1, Cause: errors.New("reserved/view schema intent differs from preserved source")}
			}
			continue
		}
		current.Tables = append(current.Tables, table)
	}
	for name := range original {
		if !seen[name] {
			return UndoRequest{}, &Error{Kind: "source-completeness", Table: name, Column: "*", Row: -1, Cause: errors.New("complete current user and reserved evidence required, including empty tables")}
		}
	}
	sort.Slice(current.Tables, func(i, j int) bool { return current.Tables[i].Name < current.Tables[j].Name })
	request := UndoRequest{Format: UndoFormat, HistoryMode: "switch-back", Source: source, Current: current, ReverseReport: bytes.Clone(report), ReverseReportBytes: bytes.Clone(report)}
	if err := validateUndo(request); err != nil {
		return UndoRequest{}, err
	}
	return request, nil
}

func sameColumns(left, right []Column) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func nativeUndoCell(cell dumpformat.NativeDeltaCell) (Cell, error) {
	out := Cell{Type: cell.Kind, Data: cell.Value}
	switch cell.Kind {
	case "sql-null":
		out.Type = "null"
	case "secret-reference", "string-bytes":
		return Cell{}, errors.New("unavailable physical value cannot be inferred")
	case "float32-bits", "float64-bits":
		width := 64
		if cell.Kind == "float32-bits" {
			width = 32
		}
		bits, err := strconv.ParseUint(cell.Value, 16, width)
		if err != nil {
			return Cell{}, err
		}
		if width == 32 {
			value := math.Float32frombits(uint32(bits))
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return Cell{}, errors.New("nonfinite physical float")
			}
			return encodeCell(value)
		}
		value := math.Float64frombits(bits)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Cell{}, errors.New("nonfinite physical float")
		}
		return encodeCell(value)
	}
	if _, err := decodeCell(out); err != nil {
		return Cell{}, err
	}
	return out, nil
}

// CanonicalPhysicalBytes is the reviewed C(M) public SQL payload byte domain.
// It never parses Cell.Data or coerces values. Raw database files, graph/root
// identity and new commit metadata are separate preservation/proof domains.
func CanonicalPhysicalBytes(manifest Manifest) ([]byte, error) {
	if _, err := decode(manifest); err != nil {
		return nil, err
	}
	out := Manifest{Format: manifest.Format, HistoryMode: manifest.HistoryMode, Tables: make([]Table, len(manifest.Tables))}
	for i, table := range manifest.Tables {
		owned := Table{Name: table.Name, Columns: append([]Column{}, table.Columns...), Rows: make([][]Cell, len(table.Rows))}
		type rowBytes struct {
			cells []Cell
			data  []byte
		}
		rows := make([]rowBytes, len(table.Rows))
		for ri, cells := range table.Rows {
			data, err := json.Marshal(cells)
			if err != nil {
				return nil, err
			}
			rows[ri] = rowBytes{append([]Cell{}, cells...), data}
		}
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].data, rows[j].data) < 0 })
		for ri, row := range rows {
			owned.Rows[ri] = row.cells
		}
		out.Tables[i] = owned
	}
	sort.Slice(out.Tables, func(i, j int) bool { return out.Tables[i].Name < out.Tables[j].Name })
	return json.Marshal(out)
}
