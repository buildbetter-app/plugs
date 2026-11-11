package embedded

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"testing"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type physicalTestChunks struct {
	chunks         map[hash.Hash]chunks.Chunk
	present        map[hash.Hash]bool
	getErr, hasErr error
	reads          map[hash.Hash]int
}

func (store *physicalTestChunks) Get(_ context.Context, address hash.Hash) (chunks.Chunk, error) {
	store.reads[address]++
	if store.getErr != nil {
		return chunks.EmptyChunk, store.getErr
	}
	if chunk, ok := store.chunks[address]; ok {
		return chunk, nil
	}
	return chunks.EmptyChunk, nil
}

func (store *physicalTestChunks) Has(_ context.Context, address hash.Hash) (bool, error) {
	return store.present[address], store.hasErr
}

func TestPhysicalPresenceProbeTraversesDescendantsAndRefusesPresentInconsistency(t *testing.T) {
	ctx := context.Background()
	serializer := message.NewBlobSerializer(pool.NewBuffPool())
	leaf := chunks.NewChunk(append([]byte{}, serializer.Serialize(nil, [][]byte{[]byte("public leaf")}, []uint64{11}, 0)...))
	leafID := leaf.Hash()
	root := chunks.NewChunk(append([]byte{}, serializer.Serialize(nil, [][]byte{leafID[:]}, []uint64{11}, 1)...))
	rootID := root.Hash()
	for _, variant := range []string{"complete", "missing-leaf", "missing-root", "present-empty", "false-presence", "hash-mismatch", "presence-error", "read-error"} {
		t.Run(variant, func(t *testing.T) {
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{rootID: root, leafID: leaf}, present: map[hash.Hash]bool{rootID: true, leafID: true}, reads: map[hash.Hash]int{}}
			wantReason := ""
			switch variant {
			case "missing-leaf":
				delete(store.chunks, leafID)
				delete(store.present, leafID)
				wantReason = "missing_table_chunk"
			case "missing-root":
				delete(store.chunks, rootID)
				delete(store.present, rootID)
				wantReason = "missing_root_chunk"
			case "present-empty":
				delete(store.chunks, leafID)
			case "false-presence":
				delete(store.present, leafID)
			case "hash-mismatch":
				store.chunks[leafID] = chunks.NewChunkWithHash(leafID, []byte("different present bytes"))
			case "presence-error":
				store.hasErr = fs.ErrPermission
			case "read-error":
				store.getErr = fs.ErrPermission
			}
			err := probePhysicalRootChunks(ctx, store, rootID)
			if variant == "complete" {
				if err != nil || store.reads[rootID] != 1 || store.reads[leafID] != 1 {
					t.Fatal("presence probe did not authenticate every descendant")
				}
				return
			}
			if err == nil || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "", err)) != wantReason {
				t.Fatal("presence probe conflated absence with a present inconsistency")
			}
			if variant == "missing-leaf" && store.reads[leafID] != 1 || variant == "missing-root" && (store.reads[rootID] != 1 || store.reads[leafID] != 0) {
				t.Fatal("probe did not confirm actual absence or followed absent root references")
			}
		})
	}
	if err := probePhysicalRootChunks(ctx, nil, hash.Hash{}); err == nil {
		t.Fatal("empty reference was admitted")
	}
}

func TestPhysicalRowReferencesUseBackingBytesAndRejectIntegrityFailures(t *testing.T) {
	ctx := context.Background()
	serializer := message.NewBlobSerializer(pool.NewBuffPool())
	leaf := chunks.NewChunk(append([]byte{}, serializer.Serialize(nil, [][]byte{[]byte("public inline value")}, []uint64{19}, 0)...))
	leafHash := leaf.Hash()
	parent := chunks.NewChunk(append([]byte{}, serializer.Serialize(nil, [][]byte{leafHash[:], leafHash[:]}, []uint64{19, 19}, 1)...))
	parentHash := parent.Hash()
	root := append([]byte{}, serializer.Serialize(nil, [][]byte{parentHash[:]}, []uint64{38}, 2)...)
	for _, test := range []struct{ name, reason string }{
		{"complete", ""},
		{"missing_leaf", "missing_table_chunk"},
		{"present_empty", ""},
		{"hash_mismatch", ""},
		{"malformed_present", ""},
		{"permission_failure", ""},
		{"presence_check_failure", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{leafHash: leaf, parentHash: parent}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			input := root
			switch test.name {
			case "missing_leaf":
				delete(store.chunks, leafHash)
			case "present_empty":
				delete(store.chunks, leafHash)
				store.present[leafHash] = true
			case "hash_mismatch":
				store.chunks[leafHash] = chunks.NewChunkWithHash(leafHash, []byte("different present bytes"))
			case "malformed_present":
				invalid := chunks.NewChunk([]byte("present malformed message"))
				address := invalid.Hash()
				store.chunks[address] = invalid
				input = append([]byte{}, serializer.Serialize(nil, [][]byte{address[:]}, []uint64{1}, 1)...)
			case "permission_failure":
				store.getErr = fs.ErrPermission
			case "presence_check_failure":
				delete(store.chunks, leafHash)
				store.hasErr = fs.ErrPermission
			}
			err := checkPhysicalRowReferences(ctx, store, input, physicalRowSchema{})
			if test.name == "complete" {
				if err != nil || store.reads[leafHash] != 1 || store.reads[parentHash] != 1 {
					t.Fatal("complete nested references failed or shared descendant read more than once")
				}
				return
			}
			if err == nil || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != test.reason {
				t.Fatal("backing-store absence and present integrity failure were conflated")
			}
			if (test.name == "permission_failure" || test.name == "presence_check_failure") && !errors.Is(err, fs.ErrPermission) {
				t.Fatal("original backing-store permission failure lost")
			}
		})
	}
	t.Run("missing_with_present_corruption_refuses", func(t *testing.T) {
		absent := hash.Of([]byte("actual absent public data"))
		bad := chunks.NewChunk([]byte("malformed present object"))
		badHash := bad.Hash()
		input := serializer.Serialize(nil, [][]byte{absent[:], badHash[:]}, []uint64{1, 1}, 1)
		store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{badHash: bad}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
		err := checkPhysicalRowReferences(ctx, store, input, physicalRowSchema{})
		if err == nil || store.reads[badHash] != 1 || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != "" {
			t.Fatal("an earlier absent leaf hid corrupt present bytes")
		}
	})
	t.Run("cancelled_empty_root_refuses", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := checkPhysicalRowReferences(cancelled, nil, nil, physicalRowSchema{}); !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled reference read was not refused")
		}
	})
}

func TestPhysicalStoredChunkRequiresAuthenticatedAddressAndActualAbsence(t *testing.T) {
	ctx := context.Background()
	address := hash.Of([]byte("authenticated absent public object"))
	for _, reason := range []string{"missing_table_chunk", "unreadable_schema"} {
		t.Run(reason, func(t *testing.T) {
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			_, err := readPhysicalVerifiedChunk(ctx, store, address, reason)
			var absent *storedChunkAbsent
			if !errors.As(err, &absent) || absent.reason != reason || store.reads[address] != 1 {
				t.Fatal("authenticated missing object evidence was lost or changed")
			}
			_, err = readPhysicalVerifiedChunk(ctx, store, hash.Hash{}, reason)
			if err == nil || errors.As(err, &absent) {
				t.Fatal("empty unauthenticated reference became positive absence")
			}
		})
	}
}

// Build raw stored FlatBuffers with independent key/value vectors. The pinned
// high-level map serializer assumes equal lengths and cannot author the malformed
// cardinality counterexample. Every stored chunk below uses its actual byte hash.
func physicalRawPrimaryNode(keys, values [][]byte, addresses []hash.Hash, counts []uint64, level byte, total uint64, valueAddressOffsets ...uint16) []byte {
	// A fixed raw FlatBuffer table makes every stored vector independent. Field
	// order follows the pinned serial.ProllyTreeNode vtable, without importing
	// the builder or invoking its high-level map serialization assumptions.
	const table = 40
	fields := []uint16{4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 48}
	data := make([]byte, table+56)
	binary.LittleEndian.PutUint32(data[4:], table-4)
	copy(data[8:12], serial.ProllyTreeNodeFileID)
	binary.LittleEndian.PutUint16(data[12:], uint16(4+2*len(fields)))
	binary.LittleEndian.PutUint16(data[14:], 56)
	binary.LittleEndian.PutUint32(data[table:], table-12)
	field := func(slot int) int {
		binary.LittleEndian.PutUint16(data[16+slot*2:], fields[slot])
		return table + int(fields[slot])
	}
	vector := func(slot int, payload []byte, count int) {
		for len(data)%4 != 0 {
			data = append(data, 0)
		}
		position := len(data)
		data = append(data, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(data[position:], uint32(count))
		data = append(data, payload...)
		offset := field(slot)
		binary.LittleEndian.PutUint32(data[offset:], uint32(position-offset))
	}
	offsets := func(items [][]byte) []byte {
		out := make([]byte, (len(items)+1)*2)
		count := 0
		for i, item := range items {
			count += len(item)
			binary.LittleEndian.PutUint16(out[(i+1)*2:], uint16(count))
		}
		return out
	}
	vector(0, bytes.Join(keys, nil), len(bytes.Join(keys, nil)))
	vector(1, offsets(keys), len(keys)+1)
	data[field(2)] = byte(serial.ItemTypeTupleFormatAlpha)
	data[field(5)] = byte(serial.ItemTypeTupleFormatAlpha)
	binary.LittleEndian.PutUint64(data[field(9):], total)
	data[field(10)] = level
	if level == 0 {
		vector(3, bytes.Join(values, nil), len(bytes.Join(values, nil)))
		vector(4, offsets(values), len(values)+1)
		if len(valueAddressOffsets) != 0 {
			raw := make([]byte, 2*len(valueAddressOffsets))
			for i, offset := range valueAddressOffsets {
				binary.LittleEndian.PutUint16(raw[2*i:], offset)
			}
			vector(6, raw, len(valueAddressOffsets))
		}
	} else {
		var raw []byte
		for _, address := range addresses {
			raw = append(raw, address[:]...)
		}
		vector(7, raw, len(raw))
		var encoded []byte
		var previous int64
		for _, count := range counts {
			var buf [binary.MaxVarintLen64]byte
			n := binary.PutVarint(buf[:], int64(count)-previous)
			encoded = append(encoded, buf[:n]...)
			previous = int64(count)
		}
		vector(8, encoded, len(encoded))
	}
	size := len(data) - serial.MessagePrefixSz
	data[0], data[1], data[2], data[3] = byte(serial.MessageTypesKind), byte(size>>16), byte(size>>8), byte(size)
	return data
}

func TestPhysicalPresentNodeStructureCannotHideBehindAbsentSibling(t *testing.T) {
	ctx := context.Background()
	a, b, value := []byte{'a', 1, 0}, []byte{'b', 1, 0}, []byte{'v', 1, 0}
	malformed := chunks.NewChunk(physicalRawPrimaryNode([][]byte{a, b}, [][]byte{value}, nil, nil, 0, 2))
	malformedHash := malformed.Hash()
	// Independently retain the old walker's blind spot: a recognized, hash-correct
	// message with no addresses is not the same as a structurally valid leaf.
	visited := 0
	if err := message.WalkAddresses(ctx, malformed.Data(), func(context.Context, hash.Hash) error { visited++; return nil }); err != nil || visited != 0 || hash.Of(malformed.Data()) != malformedHash {
		t.Fatal("raw valid-ID malformed-node control did not establish the original address-walker seam")
	}
	absent := hash.Of([]byte("actually absent public primary child"))
	for _, mixed := range []bool{false, true} {
		name := "malformed_only"
		refs, keys, counts := []hash.Hash{malformedHash}, [][]byte{b}, []uint64{2}
		total := uint64(2)
		if mixed {
			name = "actually_absent_sibling_then_present_malformed"
			refs = []hash.Hash{absent, malformedHash}
			keys = [][]byte{a, b}
			counts = []uint64{1, 2}
			total = 3
		}
		t.Run(name, func(t *testing.T) {
			root := physicalRawPrimaryNode(keys, nil, refs, counts, 1, total)
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{malformedHash: malformed}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			err := checkPhysicalRowReferences(ctx, store, root, physicalInt8TestSchema())
			if err == nil || store.reads[malformedHash] != 1 || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != "" {
				t.Fatal("recognized hash-correct malformed present node became an absent-history boundary")
			}
			if mixed {
				var missing *storedChunkAbsent
				if store.reads[absent] != 1 || !errors.As(err, &missing) {
					t.Fatal("mixed structural failure lost its independently observed actual absence cause")
				}
			}
		})
	}
	good := chunks.NewChunk(physicalRawPrimaryNode([][]byte{b}, [][]byte{value}, nil, nil, 0, 1))
	goodHash := good.Hash()
	for _, missing := range []bool{false, true} {
		name := "well_formed_complete"
		refs, keys, counts := []hash.Hash{goodHash}, [][]byte{b}, []uint64{1}
		total := uint64(1)
		if missing {
			name = "well_formed_present_sibling_and_actual_absence"
			refs = []hash.Hash{absent, goodHash}
			keys = [][]byte{a, b}
			counts = []uint64{1, 1}
			total = 2
		}
		t.Run(name, func(t *testing.T) {
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{goodHash: good}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			err := checkPhysicalRowReferences(ctx, store, physicalRawPrimaryNode(keys, nil, refs, counts, 1, total), physicalInt8TestSchema())
			want := ""
			if missing {
				want = "missing_table_chunk"
			}
			if (!missing && err != nil) || missing && err == nil || store.reads[goodHash] != 1 || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != want {
				t.Fatal("structural validation changed complete reads or genuine missing-only evidence")
			}
		})
	}
}

func TestPhysicalPresentNodeOffsetsLevelsCountsAndBlobShapeRefuse(t *testing.T) {
	ctx := context.Background()
	key, value := []byte{'a', 1, 0}, []byte{'v', 1, 0}
	for _, variant := range []string{"value_vector_offset", "tuple_field_count", "child_level", "child_count", "branch_address_width", "subtree_total", "blob_payload_size"} {
		t.Run(variant, func(t *testing.T) {
			data := physicalRawPrimaryNode([][]byte{key}, [][]byte{value}, nil, nil, 0, 1)
			var node serial.ProllyTreeNode
			if err := serial.InitProllyTreeNodeRoot(&node, data, serial.MessagePrefixSz); err != nil {
				t.Fatal(err)
			}
			level, total := byte(1), uint64(1)
			switch variant {
			case "value_vector_offset":
				node.MutateValueOffsets(1, uint16(len(data)))
			case "tuple_field_count":
				node.MutateValueItems(2, 255)
			case "child_level":
				level = 2
			case "child_count":
				total = 2
			case "blob_payload_size":
				serializer := message.NewBlobSerializer(pool.NewBuffPool())
				data = append([]byte{}, serializer.Serialize(nil, [][]byte{[]byte("public")}, []uint64{6}, 0)...)
				var blob serial.Blob
				if err := serial.InitBlobRoot(&blob, data, serial.MessagePrefixSz); err != nil {
					t.Fatal(err)
				}
				blob.MutateTreeSize(7)
			}
			child := chunks.NewChunk(data)
			address := child.Hash()
			root := physicalRawPrimaryNode([][]byte{key}, nil, []hash.Hash{address}, []uint64{total}, level, total)
			if variant == "blob_payload_size" {
				serializer := message.NewBlobSerializer(pool.NewBuffPool())
				root = append([]byte{}, serializer.Serialize(nil, [][]byte{address[:]}, []uint64{7}, 1)...)
			}
			if variant == "branch_address_width" || variant == "subtree_total" {
				var branch serial.ProllyTreeNode
				if err := serial.InitProllyTreeNodeRoot(&branch, root, serial.MessagePrefixSz); err != nil {
					t.Fatal(err)
				}
				if variant == "subtree_total" {
					branch.MutateTreeCount(2)
				} else {
					// The generated table exposes the address vector's length immediately
					// before its bytes. Change that stored raw length, preserving valid ID.
					tab := branch.Table()
					offset := int(tab.Pos) + int(tab.Offset(18))
					start := offset + int(binary.LittleEndian.Uint32(root[offset:]))
					binary.LittleEndian.PutUint32(root[start:], hash.ByteLen-1)
				}
			}
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{address: child}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			err := checkPhysicalRowReferences(ctx, store, root, physicalInt8TestSchema())
			if err == nil || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != "" {
				t.Fatal("present structural inconsistency was admitted as history absence")
			}
		})
	}
}

func physicalInt8TestSchema() physicalRowSchema {
	return physicalRowSchema{keys: val.NewTupleDescriptor(val.Type{Enc: val.Int8Enc}), values: val.NewTupleDescriptor(val.Type{Enc: val.Int8Enc, Nullable: true})}
}

func TestPhysicalPresentTuplesUseAuthenticatedSchemaBeforeAbsence(t *testing.T) {
	ctx := context.Background()
	key := []byte{1, 1, 0}
	bad := []byte{1, 1, 0} // one data byte is Int8, not nullable Int64
	good := append(make([]byte, 8), 1, 0)
	absent := hash.Of([]byte("actually absent public typed primary child"))
	schema := physicalRowSchema{keys: val.NewTupleDescriptor(val.Type{Enc: val.Int8Enc}), values: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true})}
	for _, variant := range []string{"malformed_only", "absent_then_malformed", "valid_complete", "valid_and_missing", "same_byte_valid_int8", "truncated_nullable", "null_required_refuses"} {
		t.Run(variant, func(t *testing.T) {
			value, typed := good, schema
			malformed := variant == "malformed_only" || variant == "absent_then_malformed" || variant == "null_required_refuses"
			mixed := variant == "absent_then_malformed" || variant == "valid_and_missing"
			if variant == "malformed_only" || variant == "absent_then_malformed" || variant == "same_byte_valid_int8" {
				value = bad
			}
			if variant == "same_byte_valid_int8" {
				typed = physicalInt8TestSchema()
			}
			if variant == "truncated_nullable" || variant == "null_required_refuses" {
				value = []byte{0, 0}
			}
			if variant == "null_required_refuses" {
				typed.values = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc})
			}
			leaf := chunks.NewChunk(physicalRawPrimaryNode([][]byte{key}, [][]byte{value}, nil, nil, 0, 1))
			address := leaf.Hash()
			refs, keys, counts := []hash.Hash{address}, [][]byte{key}, []uint64{1}
			if mixed {
				refs, keys, counts = []hash.Hash{absent, address}, [][]byte{{0, 1, 0}, key}, []uint64{1, 1}
			}
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{address: leaf}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			err := checkPhysicalRowReferences(ctx, store, physicalRawPrimaryNode(keys, nil, refs, counts, 1, uint64(len(refs))), typed)
			want := ""
			if mixed && !malformed {
				want = "missing_table_chunk"
			}
			if (malformed || mixed) != (err != nil) || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != want || store.reads[address] != 1 {
				t.Fatal("authenticated field encodings, nullable suffixes and actual absence were conflated")
			}
			if mixed {
				var missing *storedChunkAbsent
				if store.reads[absent] != 1 || !errors.As(err, &missing) {
					t.Fatal("typed malformed-present refusal lost its actual absent sibling cause")
				}
			}
		})
	}
}

func TestPhysicalDeferredTupleReadsPreserveMixedFailureRefusal(t *testing.T) {
	ctx := context.Background()
	serializer := message.NewBlobSerializer(pool.NewBuffPool())
	blob := chunks.NewChunk(append([]byte{}, serializer.Serialize(nil, [][]byte{[]byte("public value")}, []uint64{12}, 0)...))
	address := blob.Hash()
	schema := physicalRowSchema{keys: val.NewTupleDescriptor(val.Type{Enc: val.Int8Enc}), values: val.NewTupleDescriptor(val.Type{Enc: val.BytesAddrEnc, Nullable: true}, val.Type{Enc: val.Int64Enc, Nullable: true})}
	for _, variant := range []string{"complete", "missing_only", "missing_then_bad_field", "present_then_bad_field", "undeclared_reference"} {
		t.Run(variant, func(t *testing.T) {
			integer := make([]byte, 8)
			bad := variant == "missing_then_bad_field" || variant == "present_then_bad_field"
			if bad {
				integer = []byte{1}
			}
			tuple := append(append([]byte{}, address[:]...), integer...)
			tuple = append(tuple, byte(hash.ByteLen), 0, 2, 0)
			input := physicalRawPrimaryNode([][]byte{{1, 1, 0}}, [][]byte{tuple}, nil, nil, 0, 1, 0)
			if variant == "undeclared_reference" {
				input = physicalRawPrimaryNode([][]byte{{1, 1, 0}}, [][]byte{tuple}, nil, nil, 0, 1)
			}
			store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{address: blob}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
			missing := variant == "missing_only" || variant == "missing_then_bad_field"
			if missing {
				delete(store.chunks, address)
			}
			err := checkPhysicalRowReferences(ctx, store, input, schema)
			want := ""
			if variant == "missing_only" {
				want = "missing_table_chunk"
			}
			if (variant != "complete") != (err != nil) || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != want {
				t.Fatal("deferred original absence hid a present typed field failure or unknown read")
			}
			if variant != "undeclared_reference" && store.reads[address] != 1 {
				t.Fatal("deferred decoder bypassed the authenticated backing-byte set")
			}
			var absent *storedChunkAbsent
			if missing && !errors.As(err, &absent) {
				t.Fatal("deferred original missing cause was lost")
			}
		})
	}
}

func TestPhysicalSQLDomainsUseImmutableDefinitionsBeforeAbsence(t *testing.T) {
	ctx := sql.NewEmptyContext()
	absent := hash.Of([]byte("actually absent public SQL-domain primary child"))
	for _, domain := range []string{"enum", "set"} {
		for _, layout := range []string{"value", "primary_key", "keyless", "after_virtual_column"} {
			for _, variant := range []string{"malformed_only", "absent_then_malformed", "valid_complete", "valid_and_missing", "stored_zero", "explicit_null", "truncated_nullable", "two_member_definition", "definition_unavailable", "descriptor_mismatch"} {
				if layout == "primary_key" && (variant == "explicit_null" || variant == "truncated_nullable") {
					continue // primary keys in these actual schemas are required
				}
				t.Run(domain+"/"+layout+"/"+variant, func(t *testing.T) {
					members := []string{"allowed"}
					if variant == "two_member_definition" {
						members = append(members, "also_allowed")
					}
					var domainType, virtualType sql.Type
					width := 2
					if domain == "enum" {
						domainType = types.MustCreateEnumType(members, sql.Collation_Default)
						virtualType = types.MustCreateSetType([]string{"virtual_only"}, sql.Collation_Default)
					} else {
						domainType = types.MustCreateSetType(members, sql.Collation_Default)
						virtualType = types.MustCreateEnumType([]string{"virtual_only"}, sql.Collation_Default)
						width = 8
					}
					column := func(name string, tag uint64, typ sql.Type, key bool) doltschema.Column {
						info, err := typeinfo.FromSqlType(typ)
						if err != nil {
							t.Fatal(err)
						}
						out := doltschema.Column{Name: name, Tag: tag, Kind: info.NomsKind(), TypeInfo: info, IsPartOfPK: key}
						if key {
							out.Constraints = []doltschema.ColConstraint{doltschema.NotNullConstraint{}}
						}
						return out
					}
					columns := []doltschema.Column{column("id", 0, types.Int8, true)}
					switch layout {
					case "primary_key":
						columns = []doltschema.Column{column("domain_key", 0, domainType, true), column("payload", 1, types.Int8, false)}
					case "keyless":
						columns = []doltschema.Column{column("domain_value", 1, domainType, false)}
					case "after_virtual_column":
						virtual := column("virtual_value", 1, virtualType, false)
						virtual.Virtual = true
						columns = append(columns, virtual, column("domain_value", 2, domainType, false))
					default:
						columns = append(columns, column("domain_value", 1, domainType, false))
					}
					storage, err := doltschema.SchemaFromCols(doltschema.NewColCollection(columns...))
					if err != nil {
						t.Fatal(err)
					}
					keyDesc, valueDesc := storage.GetMapDescriptors(nil)
					typed := physicalRowSchema{keys: keyDesc, values: valueDesc, storage: storage}
					if variant == "definition_unavailable" {
						typed.storage = nil
					} else if variant == "descriptor_mismatch" {
						typed.keys = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc})
					}
					field := make([]byte, width)
					field[0] = 1
					malformed := variant == "malformed_only" || variant == "absent_then_malformed" || variant == "definition_unavailable" || variant == "descriptor_mismatch"
					if variant == "malformed_only" || variant == "absent_then_malformed" || variant == "two_member_definition" {
						field[0] = 2
					} else if variant == "stored_zero" {
						field[0] = 0
					} else if variant == "explicit_null" || variant == "truncated_nullable" {
						field = nil
					}
					key, lowKey := []byte{1, 1, 0}, []byte{0, 1, 0}
					value := []byte(val.NewTuple(pool.NewBuffPool(), field))
					if layout == "primary_key" {
						key = value
						lowKey = val.NewTuple(pool.NewBuffPool(), make([]byte, width))
						value = []byte{42, 1, 0}
					} else if layout == "keyless" {
						key, lowKey = make([]byte, 18), make([]byte, 18)
						key[0], key[16], lowKey[16] = 1, 1, 1 // stored 128-bit hash plus field count
						cardinality := make([]byte, 8)
						cardinality[0] = 1
						value = val.NewTuple(pool.NewBuffPool(), cardinality, field)
					}
					if variant == "explicit_null" {
						value = []byte{1, 0} // a NULL field, distinct from the truncated suffix
						if layout == "keyless" {
							value = append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, 8, 0, 2, 0)
						}
					}
					leaf := chunks.NewChunk(physicalRawPrimaryNode([][]byte{key}, [][]byte{value}, nil, nil, 0, 1))
					address := leaf.Hash()
					refs, keys, counts := []hash.Hash{address}, [][]byte{key}, []uint64{1}
					mixed := variant == "absent_then_malformed" || variant == "valid_and_missing"
					if mixed {
						refs, keys, counts = []hash.Hash{absent, address}, [][]byte{lowKey, key}, []uint64{1, 1}
					}
					store := &physicalTestChunks{chunks: map[hash.Hash]chunks.Chunk{address: leaf}, present: map[hash.Hash]bool{}, reads: map[hash.Hash]int{}}
					err = checkPhysicalRowReferences(ctx, store, physicalRawPrimaryNode(keys, nil, refs, counts, 1, uint64(len(refs))), typed)
					want := ""
					if mixed && !malformed {
						want = "missing_table_chunk"
					}
					if (malformed || mixed) != (err != nil) || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != want || store.reads[address] != 1 {
						t.Fatalf("actual SQL domain, supported values and missing-only evidence were conflated: %v", err)
					}
					if mixed {
						var missing *storedChunkAbsent
						if store.reads[absent] != 1 || !errors.As(err, &missing) {
							t.Fatal("SQL-domain refusal lost the independently observed absent-sibling cause")
						}
					}
				})
			}
		}
	}
}
