package dumpformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unsafe"
)

// These private values are produced only after typed-row and table commitment
// verification. Both indexes are persistent AVL trees: a successor owns new
// search paths and shares immutable rows/subtrees with every retained parent.
// Reference counts describe ownership, never source authority. The cache scope
// is an ancestor of all candidates, so transaction cleanup cannot uncharge a
// row still reachable from another branch, the previous image, or a checkpoint.
type deltaDecodedCache struct {
	ctx    context.Context
	budget *NativeDeltaBudget
}

type deltaDecodedRow struct {
	row              NativeDeltaRow
	indexKey         string
	fields           Row
	key, raw, hashed []byte
	refs             uint64
	typedBytes       uint64
	fieldBytes       uint64
	keyBytes         uint64
}

type deltaRowIndex struct {
	left, right *deltaRowIndex
	row         *deltaDecodedRow
	refs        uint64
	height      int
	count       uint64
	jsonBytes   uint64
}

type deltaDecodedTable struct {
	physical, normalized *deltaRowIndex
	table                NativeTable
	refs, jsonBytes      uint64
	viewBytes            uint64
	columnBytes          uint64
}

const deltaDecodedCacheBytes = uint64(unsafe.Sizeof(deltaDecodedCache{})) + uint64(unsafe.Sizeof(NativeDeltaBudget{}))
const deltaRowOwnershipBytes = uint64(unsafe.Sizeof(deltaDecodedRow{}))
const deltaIndexBytes = uint64(unsafe.Sizeof(deltaRowIndex{}))
const deltaTableOwnershipBytes = uint64(unsafe.Sizeof(deltaDecodedTable{}))

func deltaDecodedTransactionIndexBytes(tables int) uint64 {
	// Reserve separate name/schema, state, image and digest map slots, with
	// spare slots/control bytes, before constructing the transaction indexes.
	return 4*64 + uint64(tables)*(uint64(unsafe.Sizeof(""))+2*(4*uint64(unsafe.Sizeof(""))+uint64(unsafe.Sizeof(NativeDeltaTableSchema{}))+uint64(unsafe.Sizeof(deltaStagedTable{}))+uint64(unsafe.Sizeof(NativeTable{}))+uint64(unsafe.Sizeof(NativeDeltaTableDigest{}))+4*16))
}

func deltaDecodedStateIndexBytes(state deltaStagedState) uint64 {
	// Include the state, table-map key/value slots and the state/size indexes.
	// Strings and schema columns alias the already admitted immutable metadata.
	return uint64(unsafe.Sizeof(state)) + 64 + 2*uint64(len(state.tables))*(uint64(unsafe.Sizeof(""))+uint64(unsafe.Sizeof(deltaStagedTable{}))+16) + 2*uint64(unsafe.Sizeof("")) + 16
}

func deltaIndexHeight(n *deltaRowIndex) int {
	if n == nil {
		return 0
	}
	return n.height
}

func deltaIndexCount(n *deltaRowIndex) uint64 {
	if n == nil {
		return 0
	}
	return n.count
}

func (c *deltaDecodedCache) retainIndex(n *deltaRowIndex) *deltaRowIndex {
	if n != nil {
		n.refs++
	}
	return n
}

func (c *deltaDecodedCache) releaseRow(row *deltaDecodedRow) {
	if row == nil {
		return
	}
	row.refs--
	if row.refs != 0 {
		return
	}
	c.budget.ReleaseBytes("retained_delta_rows", row.typedBytes)
	c.budget.ReleaseBytes("normalized_fields", row.fieldBytes)
	c.budget.ReleaseBytes("physical_key_decode", row.keyBytes)
	c.budget.ReleaseBytes("decoded_row_map_keys", uint64(len(row.indexKey)))
	c.budget.ReleaseBytes("retained_row_sort_keys", uint64(cap(row.raw)))
	c.budget.ReleaseBytes("retained_row_hash_preimages", uint64(cap(row.hashed)))
	c.budget.ReleaseBytes("retained_row_ownership", deltaRowOwnershipBytes)
}

func (c *deltaDecodedCache) releaseIndex(n *deltaRowIndex) {
	if n == nil {
		return
	}
	n.refs--
	if n.refs != 0 {
		return
	}
	c.releaseIndex(n.left)
	c.releaseIndex(n.right)
	c.releaseRow(n.row)
	c.budget.ReleaseBytes("retained_row_index", deltaIndexBytes)
}

func (c *deltaDecodedCache) index(left *deltaRowIndex, row *deltaDecodedRow, right *deltaRowIndex) (*deltaRowIndex, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	count := uint64(1)
	size := uint64(len(row.raw))
	for _, child := range []*deltaRowIndex{left, right} {
		if child != nil {
			if err := deltaAddCount(&count, child.count); err != nil {
				return nil, err
			}
			if err := deltaAddCount(&size, child.jsonBytes); err != nil {
				return nil, err
			}
		}
	}
	if err := c.budget.ChargeBytes("retained_row_index", deltaIndexBytes); err != nil {
		return nil, err
	}
	height := deltaIndexHeight(left)
	if deltaIndexHeight(right) > height {
		height = deltaIndexHeight(right)
	}
	row.refs++
	return &deltaRowIndex{left: c.retainIndex(left), right: c.retainIndex(right), row: row, refs: 1, height: height + 1, count: count, jsonBytes: size}, nil
}

// Inputs are borrowed. The returned root owns exactly one reference; every
// intermediate path is admitted while its predecessor is still live.
func (c *deltaDecodedCache) balance(left *deltaRowIndex, row *deltaDecodedRow, right *deltaRowIndex) (*deltaRowIndex, error) {
	if deltaIndexHeight(left) > deltaIndexHeight(right)+1 {
		if deltaIndexHeight(left.left) >= deltaIndexHeight(left.right) {
			next, err := c.index(left.right, row, right)
			if err != nil {
				return nil, err
			}
			defer c.releaseIndex(next)
			return c.index(left.left, left.row, next)
		}
		pivot := left.right
		a, err := c.index(left.left, left.row, pivot.left)
		if err != nil {
			return nil, err
		}
		defer c.releaseIndex(a)
		b, err := c.index(pivot.right, row, right)
		if err != nil {
			return nil, err
		}
		defer c.releaseIndex(b)
		return c.index(a, pivot.row, b)
	}
	if deltaIndexHeight(right) > deltaIndexHeight(left)+1 {
		if deltaIndexHeight(right.right) >= deltaIndexHeight(right.left) {
			next, err := c.index(left, row, right.left)
			if err != nil {
				return nil, err
			}
			defer c.releaseIndex(next)
			return c.index(next, right.row, right.right)
		}
		pivot := right.left
		a, err := c.index(left, row, pivot.left)
		if err != nil {
			return nil, err
		}
		defer c.releaseIndex(a)
		b, err := c.index(pivot.right, right.row, right.right)
		if err != nil {
			return nil, err
		}
		defer c.releaseIndex(b)
		return c.index(a, pivot.row, b)
	}
	return c.index(left, row, right)
}

func deltaCompareRow(a, b *deltaDecodedRow, normalized bool) int {
	if normalized {
		if order := bytes.Compare(a.raw, b.raw); order != 0 {
			return order
		}
	}
	return strings.Compare(a.indexKey, b.indexKey)
}

func deltaFindRow(n *deltaRowIndex, key []byte) *deltaDecodedRow {
	for n != nil {
		switch order := bytes.Compare(key, n.row.key); {
		case order < 0:
			n = n.left
		case order > 0:
			n = n.right
		default:
			return n.row
		}
	}
	return nil
}

func (c *deltaDecodedCache) set(n *deltaRowIndex, row *deltaDecodedRow, normalized bool) (*deltaRowIndex, error) {
	if n == nil {
		return c.index(nil, row, nil)
	}
	order := deltaCompareRow(row, n.row, normalized)
	if order == 0 {
		return c.index(n.left, row, n.right)
	}
	child := n.left
	if order > 0 {
		child = n.right
	}
	next, err := c.set(child, row, normalized)
	if err != nil {
		return nil, err
	}
	defer c.releaseIndex(next)
	if order < 0 {
		return c.balance(next, n.row, n.right)
	}
	return c.balance(n.left, n.row, next)
}

func (c *deltaDecodedCache) remove(n *deltaRowIndex, row *deltaDecodedRow, normalized bool) (*deltaRowIndex, error) {
	if n == nil {
		return nil, fmt.Errorf("native delta retained row index differs")
	}
	order := deltaCompareRow(row, n.row, normalized)
	if order == 0 {
		if n.left == nil {
			return c.retainIndex(n.right), nil
		}
		if n.right == nil {
			return c.retainIndex(n.left), nil
		}
		minimum := n.right
		for minimum.left != nil {
			minimum = minimum.left
		}
		next, err := c.remove(n.right, minimum.row, normalized)
		if err != nil {
			return nil, err
		}
		defer c.releaseIndex(next)
		return c.balance(n.left, minimum.row, next)
	}
	child := n.left
	if order > 0 {
		child = n.right
	}
	next, err := c.remove(child, row, normalized)
	if err != nil {
		return nil, err
	}
	defer c.releaseIndex(next)
	if order < 0 {
		return c.balance(next, n.row, n.right)
	}
	return c.balance(n.left, n.row, next)
}

func (c *deltaDecodedCache) walk(n *deltaRowIndex, visit func(*deltaDecodedRow) error) error {
	if n == nil {
		return nil
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if err := c.walk(n.left, visit); err != nil {
		return err
	}
	if err := visit(n.row); err != nil {
		return err
	}
	return c.walk(n.right, visit)
}

func (c *deltaDecodedCache) decodeRow(raw []byte, schema NativeDeltaTableSchema, policy map[string]string, validateImage bool) (*deltaDecodedRow, error) {
	owner := c.budget.Scope()
	defer owner.Release()
	var row NativeDeltaRow
	if err := decodeNativeDeltaJSONWithBudget(raw, &row, owner, "retained_delta_rows"); err != nil {
		return nil, err
	}
	fields, key, err := validateNativeDeltaRowAccounted(schema, row, policy, owner)
	if err != nil {
		return nil, err
	}
	if validateImage {
		if err := validateNativeTableRowPolicy(schema.Name, fields, true); err != nil {
			return nil, err
		}
	}
	indexKey, err := nativeDeltaMapKeyAccounted(key, owner)
	if err != nil {
		return nil, err
	}
	capacity := uint64(base64.StdEncoding.DecodedLen(len(row.NormalizedJSON)))
	if err := owner.ChargeBytes("retained_row_sort_keys", capacity); err != nil {
		return nil, err
	}
	normalized, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
	if err != nil {
		return nil, err
	}
	size := uint64(16 + len(key) + 16 + len(normalized))
	for _, cell := range row.Cells {
		if err := deltaAddCount(&size, uint64(16+len(cell.Kind)+len(cell.Value))); err != nil {
			return nil, err
		}
	}
	if err := owner.ChargeBytes("retained_row_hash_preimages", size); err != nil {
		return nil, err
	}
	if err := owner.ChargeBytes("retained_row_ownership", deltaRowOwnershipBytes); err != nil {
		return nil, err
	}
	hashed := make([]byte, 0, int(size))
	appendBytes := func(data []byte) {
		hashed = binary.BigEndian.AppendUint64(hashed, uint64(len(data)))
		hashed = append(hashed, data...)
	}
	appendBytes(key)
	payloadSize := size - 16 - uint64(len(key))
	hashed = binary.BigEndian.AppendUint64(hashed, payloadSize)
	hashed = binary.BigEndian.AppendUint64(hashed, uint64(len(row.Cells)))
	for _, cell := range row.Cells {
		appendBytes([]byte(cell.Kind))
		appendBytes([]byte(cell.Value))
	}
	appendBytes(normalized)
	row.Op = "insert"
	decoded := &deltaDecodedRow{row: row, indexKey: indexKey, fields: fields, key: key, raw: normalized[:len(normalized)-1], hashed: hashed, refs: 1,
		typedBytes: owner.ReservedBytes("retained_delta_rows"), fieldBytes: owner.ReservedBytes("normalized_fields"), keyBytes: owner.ReservedBytes("physical_key_decode")}
	owner.Retain()
	return decoded, nil
}

func (c *deltaDecodedCache) retainTable(table *deltaDecodedTable) {
	if table != nil {
		table.refs++
	}
}

func (c *deltaDecodedCache) releaseTable(table *deltaDecodedTable) {
	if table == nil {
		return
	}
	table.refs--
	if table.refs != 0 {
		return
	}
	c.releaseIndex(table.physical)
	c.releaseIndex(table.normalized)
	c.budget.ReleaseBytes("retained_table_views", table.viewBytes)
	c.budget.ReleaseBytes("retained_table_columns", table.columnBytes)
	c.budget.ReleaseBytes("retained_table_ownership", deltaTableOwnershipBytes)
}

func (c *deltaDecodedCache) retainState(state deltaStagedState) {
	for _, table := range state.tables {
		c.retainTable(table.decoded)
	}
}

func (c *deltaDecodedCache) releaseState(state deltaStagedState) {
	for _, table := range state.tables {
		c.releaseTable(table.decoded)
	}
}

func (c *deltaDecodedCache) freeze(schema NativeDeltaTableSchema, physical, normalized *deltaRowIndex, expected string, validateImage bool) (*deltaDecodedTable, error) {
	owner := c.budget.Scope()
	defer owner.Release()
	n := deltaIndexCount(physical)
	if n != deltaIndexCount(normalized) {
		return nil, fmt.Errorf("native delta retained index census differs")
	}
	viewBytes := n * uint64(unsafe.Sizeof(Row(nil)))
	columnBytes := uint64(len(schema.Columns)) * uint64(unsafe.Sizeof(""))
	for _, charge := range []struct {
		phase string
		size  uint64
	}{{"retained_table_views", viewBytes}, {"retained_table_columns", columnBytes}, {"retained_table_ownership", deltaTableOwnershipBytes}} {
		if err := owner.ChargeBytes(charge.phase, charge.size); err != nil {
			return nil, err
		}
	}
	table := NativeTable{Columns: make([]string, 0, len(schema.Columns)), Rows: make([]Row, 0, int(n))}
	keyless := true
	for _, column := range schema.Columns {
		table.Columns = append(table.Columns, column.Name)
		if column.PrimaryKeyOrdinal != nil {
			keyless = false
		}
	}
	sort.Strings(table.Columns)
	if validateImage {
		if err := validateNativeTablePolicy(schema.Name, table, true); err != nil {
			return nil, err
		}
		if n > 100000 {
			return nil, fmt.Errorf("invalid native table census for %s", schema.Name)
		}
	}
	// Flat SHA256 is unchanged: stream the exact immutable per-row preimages in
	// physical-key order. Every changed table is hashed again in full. A retained
	// row avoids repeated base64/JSON decode, validation and payload allocation.
	h := sha256.New()
	_, _ = h.Write([]byte(nativeDeltaTableHashPrefix))
	id, _ := hex.DecodeString(NativeDeltaTableSchemaID(schema))
	_, _ = h.Write(id)
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], n)
	_, _ = h.Write(count[:])
	var previous *deltaDecodedRow
	var occurrence uint64
	if err := c.walk(physical, func(row *deltaDecodedRow) error {
		if previous != nil && bytes.Compare(previous.key, row.key) >= 0 {
			return fmt.Errorf("unordered delta keys")
		}
		if keyless {
			payload := row.hashed[16+len(row.key):]
			if len(row.key) != 44 {
				return fmt.Errorf("native delta keyless multiplicity differs")
			}
			digest := sha256.Sum256(payload)
			if !bytes.Equal(row.key[:4], []byte("bag\x00")) || !bytes.Equal(row.key[4:36], digest[:]) {
				return fmt.Errorf("native delta keyless payload digest differs")
			}
			if previous != nil && bytes.Equal(previous.key[:36], row.key[:36]) {
				if !bytes.Equal(previous.hashed[16+len(previous.key):], payload) {
					return fmt.Errorf("native delta keyless payload digest collision")
				}
				occurrence++
			} else {
				occurrence = 0
			}
			if binary.BigEndian.Uint64(row.key[36:]) != occurrence {
				return fmt.Errorf("native delta keyless multiplicity differs")
			}
		}
		_, _ = h.Write(row.hashed)
		previous = row
		return nil
	}); err != nil {
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return nil, fmt.Errorf("native delta reconstructed table hash differs")
	}
	// The same canonical normalized byte order used by json.Marshal is indexed
	// separately, including a physical-key tie break for equal normalized rows.
	size, err := NativeDeltaJSONSize(table)
	if err != nil {
		return nil, err
	}
	if normalized != nil {
		if err := deltaAddCount(&size, normalized.jsonBytes); err != nil {
			return nil, err
		}
		if err := deltaAddCount(&size, n-1); err != nil {
			return nil, err
		}
	}
	if err := c.walk(normalized, func(row *deltaDecodedRow) error { table.Rows = append(table.Rows, row.fields); return nil }); err != nil {
		return nil, err
	}
	result := &deltaDecodedTable{physical: c.retainIndex(physical), normalized: c.retainIndex(normalized), table: table, refs: 1, jsonBytes: size, viewBytes: viewBytes, columnBytes: columnBytes}
	owner.Retain()
	return result, nil
}

func (r *nativeDeltaReconstructor) decodedTable(open NativeDeltaTable, schema NativeDeltaTableSchema, base deltaStagedTable, replace bool) (deltaStagedTable, NativeTable, error) {
	var zero deltaStagedTable
	mode := "delta"
	if replace {
		mode = "replace"
	}
	if open.Type != "table" || open.Name != schema.Name || open.TableSchemaID != NativeDeltaTableSchemaID(schema) || open.Mode != mode || !validDeltaSHA256(open.ExpectedSHA256) {
		return zero, NativeTable{}, fmt.Errorf("native delta table binding/mode differs")
	}
	if !replace && base.decoded == nil {
		return zero, NativeTable{}, fmt.Errorf("native delta verified decoded base unavailable")
	}
	c := r.decoded
	var physical, normalized *deltaRowIndex
	if !replace {
		physical, normalized = c.retainIndex(base.decoded.physical), c.retainIndex(base.decoded.normalized)
	}
	defer func() { c.releaseIndex(physical); c.releaseIndex(normalized) }()
	work := r.budget.Scope()
	defer work.Release()
	var previous []byte
	var previousOwner *NativeDeltaBudget
	var operations uint64
	for {
		raw, kind, err := r.next()
		if err != nil {
			return zero, NativeTable{}, err
		}
		if kind == "end_table" {
			var end NativeDeltaEndTable
			if err := decodeNativeDeltaJSONWithBudget(raw, &end, work); err != nil {
				return zero, NativeTable{}, err
			}
			if end.RowOperations != operations || end.Rows != deltaIndexCount(physical) || end.Rows != open.ExpectedRows || end.SHA256 != open.ExpectedSHA256 {
				return zero, NativeTable{}, fmt.Errorf("native delta table terminal counts differ")
			}
			break
		}
		if kind != "row" {
			return zero, NativeTable{}, fmt.Errorf("native delta table row expected")
		}
		var fields struct {
			Op string `json:"op"`
		}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return zero, NativeTable{}, err
		}
		if err := func() error {
			var candidate *deltaDecodedRow
			var key []byte
			operation := work.Scope()
			defer operation.Release()
			if fields.Op == "delete" {
				var d NativeDeltaDelete
				if err := decodeNativeDeltaJSONWithBudget(raw, &d, operation, "decoded_operations"); err != nil {
					return err
				}
				var err error
				key, err = nativeDeltaKeyAccounted(d.Key, operation)
				if err != nil {
					return err
				}
			} else {
				var err error
				candidate, err = c.decodeRow(raw, schema, r.header.ConfigPolicy, r.validateImages)
				if err != nil {
					return err
				}
				defer c.releaseRow(candidate)
				key = candidate.key
			}
			if len(key) == 0 {
				return fmt.Errorf("native delta invalid row/key")
			}
			if previous != nil && bytes.Compare(previous, key) >= 0 {
				return fmt.Errorf("native delta operation keys unordered")
			}
			old := deltaFindRow(physical, key)
			switch fields.Op {
			case "insert":
				if old != nil {
					return fmt.Errorf("native delta insert key exists")
				}
			case "update":
				if replace || old == nil || nativeDeltaSameRow(old.row, candidate.row) {
					return fmt.Errorf("native delta update precondition differs")
				}
			case "delete":
				if replace || old == nil {
					return fmt.Errorf("native delta delete key absent")
				}
			default:
				return fmt.Errorf("native delta operation unsupported")
			}
			if previousOwner != nil {
				previousOwner.Release()
			}
			previousOwner = work.Scope()
			if err := previousOwner.ChargeBytes("operation_previous_key", uint64(len(key))); err != nil {
				return err
			}
			previous = bytes.Clone(key)
			for _, item := range []struct {
				root   **deltaRowIndex
				normal bool
			}{{&physical, false}, {&normalized, true}} {
				root := *item.root
				if old != nil {
					next, err := c.remove(root, old, item.normal)
					if err != nil {
						return err
					}
					c.releaseIndex(root)
					root, *item.root = next, next
				}
				if candidate != nil {
					next, err := c.set(root, candidate, item.normal)
					if err != nil {
						return err
					}
					c.releaseIndex(root)
					*item.root = next
				}
			}
			return deltaAddCount(&operations, 1)
		}(); err != nil {
			return zero, NativeTable{}, err
		}
	}
	if previousOwner != nil {
		previousOwner.Release()
	}
	var table *deltaDecodedTable
	if !replace && operations == 0 {
		if base.digest.Rows != open.ExpectedRows || base.digest.SHA256 != open.ExpectedSHA256 {
			return zero, NativeTable{}, fmt.Errorf("native delta unchanged table hash differs")
		}
		table = base.decoded
		c.retainTable(table)
	} else {
		var err error
		table, err = c.freeze(schema, physical, normalized, open.ExpectedSHA256, r.validateImages)
		if err != nil {
			return zero, NativeTable{}, err
		}
	}
	if err := r.countDecodedTable(open.ExpectedRows, uint64(len(schema.Columns)), operations); err != nil {
		c.releaseTable(table)
		return zero, NativeTable{}, err
	}
	return deltaStagedTable{schema: schema, digest: NativeDeltaTableDigest{Rows: open.ExpectedRows, SHA256: open.ExpectedSHA256}, decoded: table}, table.table, nil
}

func (r *nativeDeltaReconstructor) countDecodedTable(rows, columns, operations uint64) error {
	if columns != 0 && rows > ^uint64(0)/columns {
		return fmt.Errorf("native delta cell count overflow")
	}
	for _, count := range []struct {
		dst *uint64
		n   uint64
	}{{&r.totals.TableTransactions, 1}, {&r.totals.RowOperations, operations}, {&r.totals.ReconstructedRows, rows}, {&r.totals.ReconstructedCells, rows * columns}} {
		if err := deltaAddCount(count.dst, count.n); err != nil {
			return err
		}
	}
	return nil
}

func (c *deltaDecodedCache) readRows(table *deltaDecodedTable, budget *NativeDeltaBudget) (map[string]nativeDeltaOwnedRow, error) {
	if err := budget.ChargeBytes("callback_row_index", 64+2*deltaIndexCount(table.physical)*(uint64(unsafe.Sizeof(nativeDeltaOwnedRow{}))+uint64(unsafe.Sizeof(""))+16)); err != nil {
		return nil, err
	}
	rows := make(map[string]nativeDeltaOwnedRow, int(deltaIndexCount(table.physical)))
	err := c.walk(table.physical, func(row *deltaDecodedRow) error {
		key, err := nativeDeltaMapKeyAccounted(row.key, budget)
		if err != nil {
			return err
		}
		rows[key] = nativeDeltaOwnedRow{row: row.row}
		return nil
	})
	return rows, err
}

// Exact compact JSON size: table payloads are immutable; only the map's names,
// punctuation, image metadata and schema profile are counted for each commit.
func (r *nativeDeltaReconstructor) decodedImageSize(image NativeHistoryImage, state deltaStagedState) (uint64, error) {
	metadata := image
	metadata.Tables = map[string]NativeTable{}
	size, err := NativeDeltaJSONSize(metadata)
	if err != nil {
		return 0, err
	}
	first := true
	for name, table := range state.tables {
		if table.decoded == nil {
			return 0, fmt.Errorf("native delta decoded image table unavailable")
		}
		counter := deltaSizeCounter{}
		if err := counter.text(name); err != nil {
			return 0, err
		}
		if !first {
			if err := deltaAddCount(&size, 1); err != nil {
				return 0, err
			}
		}
		first = false
		if err := deltaAddCount(&size, counter.n+table.decoded.jsonBytes); err != nil {
			return 0, err
		}
	}
	return size, nil
}
