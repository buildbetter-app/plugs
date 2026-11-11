package embedded

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

type physicalRowReference struct {
	address hash.Hash
	kind    string
	level   int
	count   uint64
	counted bool
}

type physicalRowNode struct {
	kind         string
	level        int
	count        uint64
	references   []physicalRowReference
	keys, values []val.Tuple
}

// Address walking alone does not validate a recognized FlatBuffer. Validate
// each present node before any absent sibling may establish a history boundary.
// Only primary-map and out-of-line blob nodes belong to this immutable path.
func readPhysicalRowNode(data []byte) (physicalRowNode, error) {
	if len(data) < serial.MessagePrefixSz+8 || data[0] != byte(serial.MessageTypesKind) || int(data[1])<<16|int(data[2])<<8|int(data[3]) != len(data)-serial.MessagePrefixSz {
		return physicalRowNode{}, errors.New("invalid physical row message envelope")
	}
	switch serial.GetFileID(data) {
	case serial.ProllyTreeNodeFileID:
		return readPhysicalPrimaryNode(data)
	case serial.BlobFileID:
		return readPhysicalBlobNode(data)
	default:
		return physicalRowNode{}, errors.New("unsupported physical row reference format")
	}
}

func checkPhysicalOffsets(items []byte, count int, offset func(int) uint16) error {
	if count < 1 || offset(0) != 0 || int(offset(count-1)) != len(items) {
		return errors.New("invalid physical row vector endpoints")
	}
	previous := 0
	for i := 1; i < count; i++ {
		next := int(offset(i))
		if next < previous || next > len(items) {
			return errors.New("invalid physical row vector offsets")
		}
		previous = next
	}
	return nil
}

func checkPhysicalTuple(data []byte) error {
	if len(data) < 2 {
		return errors.New("invalid physical row tuple header")
	}
	count := int(binary.LittleEndian.Uint16(data[len(data)-2:]))
	// store/val/tuple.go limits the pinned tuple format to 4096 fields.
	if count > 4096 {
		return errors.New("physical row tuple exceeds pinned field count")
	}
	if count == 0 {
		if len(data) != 2 {
			return errors.New("invalid empty physical row tuple")
		}
		return nil
	}
	split := len(data) - 2*count
	if split < 0 {
		return errors.New("invalid physical row tuple field count")
	}
	previous := 0
	for i := 0; i < count-1; i++ {
		next := int(binary.LittleEndian.Uint16(data[split+2*i:]))
		if next < previous || next > split {
			return errors.New("invalid physical row tuple offsets")
		}
		previous = next
	}
	return nil
}

// Pinned subtree counts use signed deltas, not unsigned standalone varints.
// Require the whole vector, positive counts and a nonoverflowing advertised sum.
func physicalSubtreeCounts(data []byte, count int, total uint64) ([]uint64, error) {
	counts := make([]uint64, count)
	var previous int64
	var sum uint64
	const maxInt64 = int64(^uint64(0) >> 1)
	for i := range counts {
		delta, n := binary.Varint(data)
		if n <= 0 || delta < -previous || delta > maxInt64-previous {
			return nil, errors.New("invalid physical subtree count encoding")
		}
		previous += delta
		if previous <= 0 || uint64(previous) > ^uint64(0)-sum {
			return nil, errors.New("invalid physical subtree cardinality")
		}
		counts[i] = uint64(previous)
		sum += counts[i]
		data = data[n:]
	}
	if len(data) != 0 || sum != total {
		return nil, errors.New("inconsistent physical subtree count")
	}
	return counts, nil
}

func readPhysicalPrimaryNode(data []byte) (physicalRowNode, error) {
	var node serial.ProllyTreeNode
	if err := serial.InitProllyTreeNodeRoot(&node, data, serial.MessagePrefixSz); err != nil {
		return physicalRowNode{}, err
	}
	out := physicalRowNode{kind: serial.ProllyTreeNodeFileID, level: int(node.TreeLevel()), count: node.TreeCount()}
	keys := node.KeyItemsBytes()
	if node.KeyType() != serial.ItemTypeTupleFormatAlpha || node.ValueType() != serial.ItemTypeTupleFormatAlpha {
		return out, errors.New("invalid physical primary tuple format")
	}
	if err := checkPhysicalOffsets(keys, node.KeyOffsetsLength(), node.KeyOffsets); err != nil {
		return out, err
	}
	count := node.KeyOffsetsLength() - 1
	if count > int(^uint16(0)) {
		return out, errors.New("physical primary node exceeds pinned cardinality")
	}
	for i := 0; i < count; i++ {
		tuple := keys[node.KeyOffsets(i):node.KeyOffsets(i+1)]
		if err := checkPhysicalTuple(tuple); err != nil {
			return out, err
		}
		out.keys = append(out.keys, val.Tuple(tuple))
	}
	if out.level == 0 {
		values := node.ValueItemsBytes()
		if node.ValueOffsetsLength() != count+1 || node.AddressArrayLength() != 0 || node.SubtreeCountsLength() != 0 || out.count != uint64(count) {
			return out, errors.New("inconsistent physical primary leaf vectors")
		}
		if err := checkPhysicalOffsets(values, node.ValueOffsetsLength(), node.ValueOffsets); err != nil {
			return out, err
		}
		for i := 0; i < count; i++ {
			tuple := values[node.ValueOffsets(i):node.ValueOffsets(i+1)]
			if err := checkPhysicalTuple(tuple); err != nil {
				return out, err
			}
			out.values = append(out.values, val.Tuple(tuple))
		}
		seen := map[int]bool{}
		for i := 0; i < node.ValueAddressOffsetsLength(); i++ {
			offset := int(node.ValueAddressOffsets(i))
			if seen[offset] || offset+hash.ByteLen > len(values) {
				return out, errors.New("invalid physical primary value address offset")
			}
			contained := false
			for j := 0; j < count; j++ {
				start, stop := int(node.ValueOffsets(j)), int(node.ValueOffsets(j+1))
				fields := int(binary.LittleEndian.Uint16(values[stop-2 : stop]))
				if offset >= start && offset+hash.ByteLen <= stop-2*fields {
					contained = true
					break
				}
			}
			if !contained {
				return out, errors.New("physical value address crosses tuple data")
			}
			out.references = append(out.references, physicalRowReference{address: hash.New(values[offset : offset+hash.ByteLen]), kind: serial.BlobFileID, level: -1})
			seen[offset] = true
		}
		return out, nil
	}
	if count == 0 || node.ValueItemsLength() != 0 || node.ValueOffsetsLength() != 0 || node.ValueAddressOffsetsLength() != 0 || node.AddressArrayLength() != count*hash.ByteLen {
		return out, errors.New("inconsistent physical primary branch vectors")
	}
	counts, err := physicalSubtreeCounts(node.SubtreeCountsBytes(), count, out.count)
	if err != nil {
		return out, err
	}
	addresses := node.AddressArrayBytes()
	for i, n := range counts {
		out.references = append(out.references, physicalRowReference{address: hash.New(addresses[i*hash.ByteLen : (i+1)*hash.ByteLen]), kind: out.kind, level: out.level - 1, count: n, counted: true})
	}
	return out, nil
}

func readPhysicalBlobNode(data []byte) (physicalRowNode, error) {
	var node serial.Blob
	if err := serial.InitBlobRoot(&node, data, serial.MessagePrefixSz); err != nil {
		return physicalRowNode{}, err
	}
	out := physicalRowNode{kind: serial.BlobFileID, level: int(node.TreeLevel()), count: node.TreeSize()}
	if out.level == 0 {
		// Dolt's blob builder counts leaves, independently of payload bytes.
		if node.AddressArrayLength() != 0 || node.SubtreeSizesLength() != 0 || out.count != 1 {
			return out, errors.New("inconsistent physical blob leaf")
		}
		return out, nil
	}
	addresses := node.AddressArrayBytes()
	if node.PayloadLength() != 0 || len(addresses) == 0 || len(addresses)%hash.ByteLen != 0 {
		return out, errors.New("inconsistent physical blob branch vectors")
	}
	counts, err := physicalSubtreeCounts(node.SubtreeSizesBytes(), len(addresses)/hash.ByteLen, out.count)
	if err != nil {
		return out, err
	}
	for i, n := range counts {
		out.references = append(out.references, physicalRowReference{address: hash.New(addresses[i*hash.ByteLen : (i+1)*hash.ByteLen]), kind: out.kind, level: out.level - 1, count: n, counted: true})
	}
	return out, nil
}

func checkPhysicalRowReference(reference physicalRowReference, node physicalRowNode) error {
	if reference.kind != node.kind || reference.level >= 0 && reference.level != node.level || reference.counted && reference.count != node.count {
		return errors.New("physical row child differs from authenticated parent shape")
	}
	return nil
}

// These descriptors come from the immutable table's primary map, never from a
// current schema or a tuple's own untrusted bytes.
type physicalRowSchema struct {
	keys, values *val.TupleDesc
	nodes        tree.NodeStore
	storage      doltschema.Schema
}

type physicalTupleSchema struct {
	descriptor *val.TupleDesc
	widths     []int
	sqlTypes   []sql.Type
}

func physicalTupleSQLTypes(storage doltschema.Schema) (keys, values []sql.Type, err error) {
	if doltschema.IsKeyless(storage) {
		// The synthetic hash and multiplicity have no user SQL column.
		keys, values = []sql.Type{nil}, []sql.Type{nil}
	}
	err = storage.GetPKCols().Iter(func(_ uint64, column doltschema.Column) (bool, error) {
		keys = append(keys, column.TypeInfo.ToSqlType())
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}
	err = storage.GetNonPKCols().Iter(func(_ uint64, column doltschema.Column) (bool, error) {
		if !column.Virtual {
			values = append(values, column.TypeInfo.ToSqlType())
		}
		return false, nil
	})
	return keys, values, err
}

func samePhysicalTupleTypes(left, right *val.TupleDesc) bool {
	if left == nil || right == nil || left.Count() != right.Count() {
		return false
	}
	for i, typ := range left.Types {
		if typ != right.Types[i] {
			return false
		}
	}
	return true
}

func preparePhysicalTupleSchema(descriptor *val.TupleDesc) physicalTupleSchema {
	if descriptor == nil {
		return physicalTupleSchema{}
	}
	out := physicalTupleSchema{descriptor: descriptor.WithoutFixedAccess(), widths: make([]int, descriptor.Count())}
	for i, typ := range descriptor.Types {
		// Let the pinned descriptor determine fixed widths, including nullable
		// fields. Do not maintain a second encoding-width table here.
		typ.Nullable = false
		args := val.TupleDescriptorArgs{}
		if i < len(descriptor.Handlers) {
			args.Handlers = []val.TupleTypeHandler{descriptor.Handlers[i]}
		}
		fixed := val.NewTupleDescriptorWithArgs(args, typ).GetFixedAccess()
		if len(fixed) == 1 {
			out.widths[i] = int(fixed[0])
		}
	}
	return out
}

func checkPhysicalTypedTuple(ctx context.Context, tuple val.Tuple, schema physicalTupleSchema, nodes tree.NodeStore) ([]hash.Hash, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if schema.descriptor == nil || tuple.Count() > schema.descriptor.Count() {
		return nil, errors.New("physical tuple lacks its authenticated descriptor")
	}
	var references []hash.Hash
	var missing error
	for i, typ := range schema.descriptor.Types {
		var sqlType sql.Type
		if i < len(schema.sqlTypes) {
			sqlType = schema.sqlTypes[i]
		}
		if typ.Enc == val.EnumEnc {
			if _, ok := sqlType.(sql.EnumType); !ok {
				return nil, errors.New("physical enum lacks its immutable SQL definition")
			}
		} else if typ.Enc == val.SetEnc {
			if _, ok := sqlType.(sql.SetType); !ok {
				return nil, errors.New("physical set lacks its immutable SQL definition")
			}
		}
		field := tuple.GetField(i)
		if len(field) == 0 {
			// Trailing nullable fields are legitimately absent from stored tuples.
			if !typ.Nullable {
				return nil, errors.New("physical tuple has NULL in a required field")
			}
			continue
		}
		if width := schema.widths[i]; width > 0 && len(field) != width {
			return nil, errors.New("physical field width differs from authenticated descriptor")
		}
		if val.IsAddrEncoding(typ.Enc) {
			address := hash.New(field)
			if !address.IsEmpty() {
				references = append(references, address)
			}
		} else if val.IsAdaptiveEncoding(typ.Enc) && val.AdaptiveValue(field).IsOutOfBand() {
			address, err := val.AdaptiveValue(field).OutOfBandAddr()
			if err != nil {
				return nil, err
			}
			if address.IsEmpty() {
				return nil, errors.New("physical adaptive field has an empty address")
			}
			references = append(references, address)
		}
		// Use exactly the pinned row decoder and force deferred values to read
		// the already authenticated byte set. The immutable store below cannot
		// fall back to the engine's shared cache or invent a missing reference.
		value, err := tree.GetField(ctx, schema.descriptor, i, tuple, nodes)
		if err == nil {
			if jsonValue, ok := value.(sql.JSONWrapper); ok {
				_, err = jsonValue.ToInterface(ctx)
			} else {
				_, err = sql.UnwrapAny(ctx, value)
			}
		}
		if err == nil && sqlType != nil {
			sqlCtx, ok := ctx.(*sql.Context)
			if !ok {
				sqlCtx = sql.NewContext(ctx)
			}
			// Use the existing complete reader's conversion and detached-value
			// checks, including the actual ENUM/SET dictionary. Encoding widths
			// alone cannot prove these SQL domains. NULLs were handled above.
			_, err = readPhysicalRows(sqlCtx, sql.Schema{&sql.Column{Type: sqlType}}, sql.RowsToRowIter(sql.Row{value}))
		}
		if err != nil {
			if missingPhysicalHistory("rows", err) != "missing_table_chunk" {
				return nil, err
			}
			// Inspect all other present fields even when one value refers to an
			// actually absent chunk. A later schema failure must still refuse.
			missing = err
		}
	}
	return references, missing
}

func checkPhysicalTypedNodes(ctx context.Context, nodes []physicalRowNode, schema physicalRowSchema, store tree.NodeStore) error {
	var keySQL, valueSQL []sql.Type
	if schema.storage != nil {
		// Rebind extended handlers to the verified byte store as well. Their
		// original descriptor may close over the engine's shared NodeStore.
		keys, values := schema.storage.GetMapDescriptors(store)
		if !samePhysicalTupleTypes(schema.keys, keys) || !samePhysicalTupleTypes(schema.values, values) {
			return errors.New("immutable SQL schema differs from physical map descriptors")
		}
		schema.keys, schema.values = keys, values
		var err error
		keySQL, valueSQL, err = physicalTupleSQLTypes(schema.storage)
		if err != nil {
			return err
		}
		if len(keySQL) != keys.Count() || len(valueSQL) != values.Count() {
			return errors.New("immutable SQL columns differ from physical tuple positions")
		}
	}
	keys, values := preparePhysicalTupleSchema(schema.keys), preparePhysicalTupleSchema(schema.values)
	keys.sqlTypes, values.sqlTypes = keySQL, valueSQL
	for _, node := range nodes {
		if node.kind != serial.ProllyTreeNodeFileID {
			continue
		}
		for _, tuple := range node.keys {
			refs, err := checkPhysicalTypedTuple(ctx, tuple, keys, store)
			if len(refs) != 0 {
				return errors.Join(err, errors.New("unsupported out-of-line primary key"))
			}
			if err != nil {
				return err
			}
		}
		declared, actual := map[hash.Hash]int{}, map[hash.Hash]int{}
		if node.level != 0 {
			continue
		}
		for _, reference := range node.references {
			declared[reference.address]++
		}
		for _, tuple := range node.values {
			refs, err := checkPhysicalTypedTuple(ctx, tuple, values, store)
			if err != nil && missingPhysicalHistory("rows", err) != "missing_table_chunk" {
				return err
			}
			for _, address := range refs {
				actual[address]++
			}
		}
		if len(declared) != len(actual) {
			return errors.New("physical address vector differs from authenticated fields")
		}
		for address, count := range actual {
			if declared[address] != count {
				return errors.New("physical address vector differs from authenticated fields")
			}
		}
	}
	return nil
}
