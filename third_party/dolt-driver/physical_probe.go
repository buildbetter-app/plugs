package embedded

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// PhysicalHistoryProbe contains detached counts only. A readable root means
// its addressed root value decoded, not that every descendant table/row is
// readable. Layout counts are the engine Sources' indexed chunk entries, not
// unique reachable chunks and not a completeness test. Nil means unmeasured.
type PhysicalHistoryProbe struct {
	ReadOnlyScope                      bool
	ReachableCommitCount               uint64
	ReachableCountIsLowerBound         bool
	ReadableRootCount                  uint64
	ChunkCompleteCommitCount           uint64
	FirstUnreadableOrdinal             *uint64
	FirstUnreadableSchemaVersion       *uint64
	ShallowGhostMarkerCount            uint64
	ArchiveChunkCount, LooseChunkCount *uint64
	ArchiveFileCount, LooseFileCount   *uint64
	ReasonSubcode                      *string
	GoErrorTypes                       []string
	SQLDoltCodes                       []uint16
	// These remain inside the capture adapter, never in counts-only output.
	FirstUnreadableCommitID string `json:"-"`
	FirstUnreadableCause    error  `json:"-"`
}

// ReadPhysicalHistoryProbe walks parent edges and backing reference closures.
// Complete roots need no SQL row/cursor reader. An incomplete root retains the
// present-sibling validation guards and reads only fixed cursors for its schema
// coordinate; neither operation calls PartitionRows.
func ReadPhysicalHistoryProbe(ctx context.Context, conn *sql.Conn, head string) (result PhysicalHistoryProbe, resultErr error) {
	if !physicalCommitID.MatchString(head) {
		return result, physicalReadError("invalid-commit", "", "", errors.New("exact HEAD required"))
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok || d.gmsCtx.GetTransaction() != nil {
			return physicalReadError("occupied-transaction", head, "", errors.New("dedicated idle embedded connection required"))
		}
		query, _, rows, err := d.queryWithBindings(ctx, "START TRANSACTION READ ONLY", nil)
		if err != nil {
			return physicalReadError("transaction", head, "", err)
		}
		rollback := doltTx{ctx: d.gmsCtx.Context, conn: d}
		defer func() {
			err := rollback.Rollback()
			if err == nil && d.gmsCtx.GetTransaction() != nil {
				err = errors.New("probe transaction remains active")
			}
			if err != nil {
				scopeErr = errors.Join(scopeErr, physicalReadError("rollback", head, "", err))
			}
		}()
		if err := rows.Close(query); err != nil {
			return physicalReadError("transaction", head, "", err)
		}
		if tx := d.gmsCtx.GetTransaction(); tx == nil || !tx.IsReadOnly() {
			return physicalReadError("transaction", head, "", errors.New("read-only transaction not authenticated"))
		}
		result, scopeErr = d.readPhysicalHistoryProbe(ctx, head)
		if scopeErr == nil && result.FirstUnreadableCommitID != "" && result.ReasonSubcode != nil && *result.ReasonSubcode != "missing_root_chunk" && *result.ReasonSubcode != "ghost_parent" {
			// Only fixed cursor tables are read, directly from their prolly maps.
			// No PartitionRows call is made on the incomplete commit.
			cursor, err := d.readPhysicalCommitSchema(ctx, result.FirstUnreadableCommitID)
			if err == nil {
				result.FirstUnreadableSchemaVersion, scopeErr = physicalProbeSchemaVersion(cursor)
			} else if PhysicalHistoryUnavailableReason(err) == "" {
				scopeErr = err
			}
		}
		if scopeErr == nil {
			result.ReadOnlyScope = true
		}
		return scopeErr
	})
	if resultErr != nil {
		result = PhysicalHistoryProbe{}
	}
	return
}

func (d *DoltConn) readPhysicalHistoryProbe(ctx context.Context, head string) (PhysicalHistoryProbe, error) {
	q, err := d.beginQuery(ctx, "counts-only physical history probe")
	if err != nil {
		return PhysicalHistoryProbe{}, physicalReadError("query", head, "", err)
	}
	defer d.endQuery(q)
	if q.Client().User != "root" {
		return PhysicalHistoryProbe{}, physicalReadError("permission", head, "", errors.New("local root required"))
	}
	ddb, ok := dsess.DSessFromSess(q.Session).GetDoltDB(q, q.GetCurrentDatabase())
	if !ok {
		return PhysicalHistoryProbe{}, physicalReadError("database", head, "", errors.New("Dolt database unavailable"))
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	before, err := store.Root(ctx)
	if err != nil {
		return PhysicalHistoryProbe{}, physicalReadError("census", head, "", err)
	}
	result := PhysicalHistoryProbe{GoErrorTypes: []string{}, SQLDoltCodes: []uint16{}}
	if source, ok := store.(chunks.TableFileStore); ok {
		sources, err := source.Sources(ctx)
		if err != nil {
			return PhysicalHistoryProbe{}, physicalReadError("census", head, "", err)
		}
		if sources.Root != before {
			return PhysicalHistoryProbe{}, errors.New("probe source root changed")
		}
		var ac, lc, af, lf uint64
		known := true
		seen := map[string]bool{}
		for _, file := range sources.TableFiles {
			key := file.LocationPrefix() + file.FileID() + file.LocationSuffix()
			if seen[key] || file.NumChunks() < 0 {
				return PhysicalHistoryProbe{}, errors.New("inconsistent probe table-file census")
			}
			seen[key] = true
			switch file.LocationSuffix() {
			case nbs.ArchiveFileSuffix:
				af++
				ac += uint64(file.NumChunks())
			case "":
				lf++
				lc += uint64(file.NumChunks())
			default:
				known = false
			}
		}
		if known {
			result.ArchiveChunkCount, result.LooseChunkCount, result.ArchiveFileCount, result.LooseFileCount = &ac, &lc, &af, &lf
		}
	}
	spec, err := doltdb.NewCommitSpec(head)
	if err != nil {
		return PhysicalHistoryProbe{}, physicalReadError("resolve", head, "", err)
	}
	optional, err := ddb.Resolve(q, spec, nil)
	if err != nil {
		return PhysicalHistoryProbe{}, physicalReadError("resolve", head, "", err)
	}
	state := map[string]int{}
	missing := map[string]string{}
	causes := map[string]error{}
	order := []string{}
	types, codes := map[string]bool{}, map[uint16]bool{}
	observeMissing := func(id, reason string, err error) {
		missing[id] = reason
		kind := "rows"
		if reason == "missing_root_chunk" {
			kind = "root"
		} else if reason == "ghost_parent" || reason == "other" {
			kind = "resolve"
		}
		causes[id] = physicalReadError(kind, id, "", err)
		for _, name := range physicalErrorTypes(err) {
			types[name] = true
		}
		for _, code := range physicalHistorySQLCodes(err) {
			codes[code] = true
		}
	}
	var visit func(*doltdb.OptionalCommit, string) error
	visit = func(optional *doltdb.OptionalCommit, id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if state[id] == 1 {
			return errors.New("probe ancestry cycle")
		}
		if state[id] == 2 {
			return nil
		}
		if optional == nil || len(state) >= 20000 {
			return errors.New("probe ancestry absent observation or limit")
		}
		state[id] = 1
		commit, present := optional.ToCommit()
		if !present {
			result.ShallowGhostMarkerCount++
			result.ReachableCountIsLowerBound = true
			observeMissing(id, "ghost_parent", doltdb.ErrGhostCommitEncountered)
		} else {
			actual, err := commit.HashOf()
			if err != nil {
				return physicalReadError("identity", id, "", err)
			}
			if actual.String() != id {
				return errors.New("probe commit identity differs")
			}
			if root, err := readPhysicalRoot(q, ddb, commit); err != nil {
				reason := missingPhysicalHistory("root", err)
				if reason == "" {
					return physicalReadError("root", id, "", err)
				}
				observeMissing(id, reason, err)
			} else {
				result.ReadableRootCount++
				address, err := datas.GetCommitRootHash(commit.Value())
				if err != nil {
					return physicalReadError("root", id, "", err)
				}
				if err := probePhysicalRootChunks(q, store, address); err != nil {
					reason := missingPhysicalHistory("rows", err)
					if reason == "" {
						return physicalReadError("rows", id, "", err)
					}
					// Retain the prior present-sibling/domain guards before
					// admitting an absence. These use authenticated prolly
					// references, never table.PartitionRows.
					if validationErr := checkPhysicalIncompleteRoot(q, ddb, root); validationErr != nil {
						validationReason := missingPhysicalHistory("rows", validationErr)
						if validationReason == "" {
							return physicalReadError("rows", id, "", errors.Join(err, validationErr))
						}
						if validationReason == "unreadable_schema" {
							reason, err = validationReason, validationErr
						}
					}
					observeMissing(id, reason, err)
				} else {
					result.ChunkCompleteCommitCount++
				}
			}
			parents, err := commit.ParentHashes(q)
			if err != nil {
				return physicalReadError("parents", id, "", err)
			}
			indices := make([]int, len(parents))
			for i := range indices {
				indices[i] = i
			}
			sort.Slice(indices, func(i, j int) bool { return parents[indices[i]].String() < parents[indices[j]].String() })
			previousParent := ""
			for _, i := range indices {
				parentID := parents[i].String()
				if parentID == previousParent {
					return errors.New("probe duplicate parent edge")
				}
				previousParent = parentID
				parent, err := commit.GetParent(q, i)
				if err != nil {
					reason := missingPhysicalHistory("resolve", err)
					if reason == "" {
						return physicalReadError("parents", parentID, "", err)
					}
					if state[parentID] == 1 {
						return errors.New("probe ancestry cycle")
					}
					if state[parentID] != 2 {
						if len(state) >= 20000 {
							return errors.New("probe ancestry limit")
						}
						state[parentID] = 2
						order = append(order, parentID)
						observeMissing(parentID, reason, err)
					}
					result.ReachableCountIsLowerBound = true
					continue
				}
				if err := visit(parent, parentID); err != nil {
					return err
				}
			}
		}
		state[id] = 2
		order = append(order, id)
		return nil
	}
	if err := visit(optional, head); err != nil {
		return PhysicalHistoryProbe{}, err
	}
	result.ReachableCommitCount = uint64(len(order))
	for i := len(order) - 1; i >= 0; i-- {
		if reason, missing := missing[order[i]]; missing {
			ordinal := uint64(len(order) - i)
			result.FirstUnreadableOrdinal, result.ReasonSubcode = &ordinal, &reason
			result.FirstUnreadableCommitID, result.FirstUnreadableCause = order[i], causes[order[i]]
			break
		}
	}
	for name := range types {
		result.GoErrorTypes = append(result.GoErrorTypes, name)
	}
	for code := range codes {
		result.SQLDoltCodes = append(result.SQLDoltCodes, code)
	}
	sort.Strings(result.GoErrorTypes)
	sort.Slice(result.SQLDoltCodes, func(i, j int) bool { return result.SQLDoltCodes[i] < result.SQLDoltCodes[j] })
	after, err := store.Root(ctx)
	if err != nil {
		return PhysicalHistoryProbe{}, physicalReadError("census", head, "", err)
	}
	if before != after {
		return PhysicalHistoryProbe{}, errors.New("probe source root changed")
	}
	return result, nil
}

// On an incomplete closure, a malformed present sibling must still refuse.
// Complete probes stay presence-only; semantic read failures there are handled
// by the reader's typed inconsistent-store boundary.
func checkPhysicalIncompleteRoot(ctx context.Context, ddb *doltdb.DoltDB, root doltdb.RootValue) error {
	names, err := root.GetTableNames(ctx, "", false)
	if err != nil {
		return err
	}
	sort.Strings(names)
	var missing error
	for _, name := range names {
		stored, present, err := readPhysicalStoredTable(ctx, ddb, root, doltdb.TableName{Name: name})
		if err == nil && !present {
			err = errors.New("enumerated physical table absent from root")
		}
		if err == nil {
			err = checkPhysicalSchemaChunk(ctx, ddb, stored)
		}
		if err == nil {
			err = checkPhysicalTableRowChunks(ctx, ddb, stored)
		}
		if err == nil {
			continue
		}
		if missingPhysicalHistory("rows", err) == "" {
			return errors.Join(missing, err)
		}
		if missing == nil {
			missing = err
		}
	}
	return missing
}

// Probe actual backing presence for the complete immutable-root reference
// closure, including table records, primary/secondary indexes and value blobs.
// It walks stored references without constructing SQL row iterators or decoding
// user cells. A present integrity failure dominates an absent sibling.
func probePhysicalRootChunks(ctx context.Context, store physicalChunkReader, root hash.Hash) error {
	seen := map[hash.Hash]bool{root: true}
	queue := []hash.Hash{root}
	var missing error
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(queue) > 2000000 {
			return errors.New("physical presence closure exceeds limit")
		}
		address := queue[i]
		if address.IsEmpty() {
			return errors.New("physical presence closure has an empty address")
		}
		reason := "missing_table_chunk"
		if i == 0 {
			reason = "missing_root_chunk"
		}
		present, err := store.Has(ctx, address)
		if err != nil {
			return err
		}
		chunk, err := readPhysicalVerifiedChunk(ctx, store, address, reason)
		var absent *storedChunkAbsent
		if errors.As(err, &absent) && !present {
			if missing == nil {
				missing = absent
			}
			continue
		}
		if err == nil && !present || errors.As(err, &absent) && present {
			err = errors.New("physical chunk presence changed or disagrees with stored bytes")
		}
		if err == nil {
			err = types.WalkAddrsFromNomsValue(chunk, types.Format_DOLT, func(child hash.Hash) error {
				if child.IsEmpty() {
					return errors.New("physical presence closure has an empty child")
				}
				if !seen[child] {
					seen[child] = true
					queue = append(queue, child)
				}
				return nil
			})
		}
		if err != nil {
			if missing != nil {
				return errors.Join(missing, err)
			}
			return err
		}
	}
	return missing
}

func physicalProbeSchemaVersion(image PhysicalCommitSchema) (*uint64, error) {
	for _, table := range image.Tables {
		if table.Name != "schema_migrations" {
			continue
		}
		column := -1
		for i, c := range table.Columns {
			if c.Name == "version" {
				column = i
			}
		}
		if column < 0 {
			return nil, errors.New("physical probe cursor has no version column")
		}
		var version uint64
		for _, row := range table.Rows {
			var value uint64
			if column >= len(row) {
				return nil, errors.New("physical probe cursor has invalid width")
			}
			v := reflect.ValueOf(row[column])
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if v.Int() < 0 {
					return nil, errors.New("physical probe cursor is negative")
				}
				value = uint64(v.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value = v.Uint()
			default:
				return nil, errors.New("physical probe cursor is not an integer")
			}
			if value > version {
				version = value
			}
		}
		return &version, nil
	}
	return nil, nil
}
