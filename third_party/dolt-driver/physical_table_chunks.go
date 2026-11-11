package embedded

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// Authenticate a table's expected address before the engine can substitute a
// cached value or return an untyped missing-value error.
func readPhysicalStoredTable(ctx context.Context, ddb *doltdb.DoltDB, root doltdb.RootValue, name doltdb.TableName) (*doltdb.Table, bool, error) {
	address, present, err := root.GetTableHash(ctx, name)
	if err != nil || !present {
		return nil, present, err
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	if _, err := readPhysicalVerifiedChunk(ctx, store, address, "missing_table_chunk"); err != nil {
		return nil, false, err
	}
	table, present, err := root.GetTable(ctx, name)
	if err != nil || !present {
		return nil, present, err
	}
	if table == nil {
		return nil, false, errors.New("authenticated physical table returned no value")
	}
	actual, err := table.HashOf()
	if err != nil {
		return nil, false, err
	}
	if actual != address {
		return nil, false, errors.New("physical table differs from authenticated root reference")
	}
	return table, true, nil
}

func checkPhysicalSchemaChunk(ctx context.Context, ddb *doltdb.DoltDB, table *doltdb.Table) error {
	address, err := table.GetSchemaHash(ctx)
	if err != nil {
		return err
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	_, err = readPhysicalVerifiedChunk(ctx, store, address, "unreadable_schema")
	return err
}

// The pinned engine eagerly fetches row leaves and may panic on an absent
// child rather than returning a typed error. Inspect the authenticated primary
// index references first, against this store's actual bytes, without the shared
// node cache. This is only the immutable reader; ordinary SQL stays unchanged.
func checkPhysicalTableRowChunks(ctx context.Context, ddb *doltdb.DoltDB, table *doltdb.Table) error {
	storage, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}
	index, err := table.GetRowData(ctx)
	if err != nil {
		return err
	}
	rows, err := durable.ProllyMapFromIndex(index)
	if err != nil {
		return err
	}
	root, ok := tree.ValueFromNode(rows.Node()).(types.SerialMessage)
	if !ok {
		return errors.New("physical primary index has unsupported storage format")
	}
	if serial.GetFileID(root) != serial.ProllyTreeNodeFileID {
		return errors.New("physical primary index is not a primary-map node")
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	return checkPhysicalRowReferences(ctx, store, []byte(root), physicalRowSchema{keys: rows.KeyDesc(), values: rows.ValDesc(), nodes: rows.NodeStore(), storage: storage})
}

type physicalChunkReader interface {
	Get(context.Context, hash.Hash) (chunks.Chunk, error)
	Has(context.Context, hash.Hash) (bool, error)
}

func readPhysicalVerifiedChunk(ctx context.Context, store physicalChunkReader, address hash.Hash, reason string) (chunks.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return chunks.EmptyChunk, err
	}
	if address.IsEmpty() {
		return chunks.EmptyChunk, errors.New("physical chunk reference is empty")
	}
	chunk, err := store.Get(ctx, address)
	if err != nil {
		return chunks.EmptyChunk, err
	}
	if chunk.IsEmpty() {
		present, err := store.Has(ctx, address)
		if err != nil {
			return chunks.EmptyChunk, err
		}
		if present {
			return chunks.EmptyChunk, errors.New("physical chunk present but unreadable")
		}
		return chunks.EmptyChunk, &storedChunkAbsent{reason: reason}
	}
	if hash.Of(chunk.Data()) != address {
		return chunks.EmptyChunk, errors.New("physical chunk hash mismatch")
	}
	return chunk, nil
}

type physicalRowDecodePanic struct {
	cause     error
	valueType string
}

func (err *physicalRowDecodePanic) Error() string {
	return "physical row reference decode panic of type " + err.valueType
}
func (err *physicalRowDecodePanic) Unwrap() error { return err.cause }

// The embedded primary-index root is authenticated by the immutable table.
// Every descendant, including out-of-line row values, is fetched and checked
// from that root. Get(empty) with Has(true), bad bytes and malformed messages
// are integrity failures, never positive missing-history evidence.
func checkPhysicalRowReferences(ctx context.Context, store physicalChunkReader, root []byte, schema physicalRowSchema) (resultErr error) {
	var missing error
	defer func() {
		if value := recover(); value != nil {
			// Malformed present bytes can make the pinned decoder panic. Retain
			// its type only and refuse; panic strings can contain source data.
			cause, _ := value.(error)
			resultErr = &physicalRowDecodePanic{cause: cause, valueType: fmt.Sprintf("%T", value)}
			if missing != nil {
				resultErr = errors.Join(missing, resultErr)
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	withMissing := func(err error) error {
		if missing == nil {
			return err
		}
		return errors.Join(missing, err)
	}
	rootNode, err := readPhysicalRowNode(root)
	if err != nil {
		return err
	}
	presentNodes := []physicalRowNode{rootNode}
	verified := &physicalRowReadStore{NodeStore: schema.nodes, chunks: map[hash.Hash]chunks.Chunk{}, absent: map[hash.Hash]error{}}
	expected := map[hash.Hash]physicalRowReference{}
	observed := map[hash.Hash]physicalRowNode{}
	queue := []hash.Hash{}
	enqueue := func(reference physicalRowReference) error {
		if reference.address.IsEmpty() {
			return errors.New("physical row reference is empty")
		}
		if prior, ok := expected[reference.address]; ok {
			if prior.kind != reference.kind || prior.level >= 0 && reference.level >= 0 && prior.level != reference.level || prior.counted && reference.counted && prior.count != reference.count {
				return errors.New("conflicting authenticated physical child references")
			}
			if reference.counted {
				expected[reference.address] = reference
			}
			if node, ok := observed[reference.address]; ok {
				return checkPhysicalRowReference(reference, node)
			}
			return nil
		}
		expected[reference.address] = reference
		queue = append(queue, reference.address)
		return nil
	}
	for _, reference := range rootNode.references {
		if err := enqueue(reference); err != nil {
			return err
		}
	}
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return withMissing(err)
		}
		address := queue[i]
		chunk, err := readPhysicalVerifiedChunk(ctx, store, address, "missing_table_chunk")
		if err != nil {
			if absent, ok := err.(*storedChunkAbsent); ok {
				missing = absent
				verified.absent[address] = absent
				continue
			}
			return withMissing(err)
		}
		node, err := readPhysicalRowNode(chunk.Data())
		if err != nil {
			return withMissing(err)
		}
		if err := checkPhysicalRowReference(expected[address], node); err != nil {
			return withMissing(err)
		}
		observed[address] = node
		verified.chunks[address] = chunk
		presentNodes = append(presentNodes, node)
		for _, reference := range node.references {
			if err := enqueue(reference); err != nil {
				return withMissing(err)
			}
		}
	}
	if err := checkPhysicalTypedNodes(ctx, presentNodes, schema, verified); err != nil {
		return withMissing(err)
	}
	return missing
}

// Decoders can read only chunks authenticated during the backing-store walk.
// Embedded NodeStore methods provide the original immutable map's pool/format;
// every byte read bypasses its shared cache, and writes are explicitly refused.
type physicalRowReadStore struct {
	tree.NodeStore
	chunks map[hash.Hash]chunks.Chunk
	absent map[hash.Hash]error
}

func (store *physicalRowReadStore) Read(ctx context.Context, address hash.Hash) (*tree.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.absent[address]; err != nil {
		return nil, err
	}
	chunk, ok := store.chunks[address]
	if !ok {
		return nil, errors.New("decoder requested an unauthenticated physical row reference")
	}
	node, _, err := tree.NodeFromChunk(&chunk)
	return node, err
}

func (store *physicalRowReadStore) ReadMany(ctx context.Context, addresses hash.HashSlice) ([]*tree.Node, error) {
	nodes := make([]*tree.Node, len(addresses))
	for i, address := range addresses {
		node, err := store.Read(ctx, address)
		if err != nil {
			return nil, err
		}
		nodes[i] = node
	}
	return nodes, nil
}

func (store *physicalRowReadStore) ReadBytes(ctx context.Context, address hash.Hash) ([]byte, error) {
	node, err := store.Read(ctx, address)
	if err != nil {
		return nil, err
	}
	var data []byte
	err = tree.WalkNodes(ctx, node, store, func(_ context.Context, current *tree.Node) error {
		if current.IsLeaf() {
			data = append(data, current.GetValue(0)...)
		}
		return nil
	})
	return data, err
}

func (*physicalRowReadStore) Write(context.Context, *tree.Node) (hash.Hash, error) {
	return hash.Hash{}, errors.New("physical row validation cannot write nodes")
}

func (*physicalRowReadStore) WriteBytes(context.Context, []byte) (hash.Hash, error) {
	return hash.Hash{}, errors.New("physical row validation cannot write bytes")
}
