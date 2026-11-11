package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/go-sql-driver/mysql"
)

// PhysicalHistoryUnavailable is positive, typed evidence of unavailable stored
// history. An arbitrary decoding, permission or integrity error is never this
// type. Its cause remains a translated driver error, without engine objects.
type PhysicalHistoryUnavailable struct {
	Reason   string
	Cause    error
	SQLCodes []uint16
}

func (*PhysicalHistoryUnavailable) Error() string   { return "physical history unavailable" }
func (e *PhysicalHistoryUnavailable) Unwrap() error { return e.Cause }

func PhysicalHistoryUnavailableReason(err error) string {
	var inconsistent *PhysicalStoreInconsistent
	if errors.As(err, &inconsistent) {
		return ""
	}
	for depth := 0; err != nil && depth < 32; depth++ {
		if missing, ok := err.(*PhysicalHistoryUnavailable); ok {
			return missing.Reason
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			var child error
			for _, next := range wrapped.Unwrap() {
				if next != nil {
					if child != nil {
						return ""
					}
					child = next
				}
			}
			err = child
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		default:
			return ""
		}
	}
	return ""
}

func PhysicalHistoryUnavailableCodes(err error) []uint16 {
	var missing *PhysicalHistoryUnavailable
	if errors.As(err, &missing) {
		return append([]uint16{}, missing.SQLCodes...)
	}
	return nil
}

// Only numbers present in the original error are retained. translateError's
// generic fallback number is not evidence of a source SQL/Dolt error number.
func physicalHistorySQLCodes(err error) []uint16 {
	codes := map[uint16]bool{}
	var visit func(error, int)
	visit = func(e error, depth int) {
		if e == nil || depth > 32 {
			return
		}
		if sqlError, ok := e.(*mysql.MySQLError); ok && sqlError.Number != 0 {
			codes[sqlError.Number] = true
		}
		switch wrapped := e.(type) {
		case interface{ Number() int }:
			if code := wrapped.Number(); code > 0 && code <= 65535 {
				codes[uint16(code)] = true
			}
		}
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, c := range wrapped.Unwrap() {
				visit(c, depth+1)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	result := []uint16{}
	for code := range codes {
		result = append(result, code)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func missingPhysicalHistory(kind string, err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ""
	}
	var cleanup *physicalCleanupFailure
	var inconsistent *PhysicalStoreInconsistent
	if errors.As(err, &cleanup) || errors.As(err, &inconsistent) {
		return ""
	}
	// A joined integrity/cleanup error cannot become a missing-storage success
	// merely because one of its other causes is a missing-hash sentinel.
	for cause, depth := err, 0; cause != nil && depth < 32; depth++ {
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			return ""
		case interface{ Unwrap() error }:
			cause = wrapped.Unwrap()
		default:
			cause = nil
		}
	}
	if kind == "resolve" && errors.Is(err, doltdb.ErrGhostCommitEncountered) {
		return "ghost_parent"
	}
	var absent *storedChunkAbsent
	if errors.As(err, &absent) {
		if kind == "schema" && absent.reason == "missing_table_chunk" {
			return "unreadable_schema"
		}
		return absent.reason
	}
	var archive *fs.PathError
	if errors.As(err, &archive) && errors.Is(err, fs.ErrNotExist) && filepath.Ext(archive.Path) == nbs.ArchiveFileSuffix {
		switch kind {
		case "resolve", "root", "root-lock", "census", "table", "rows", "schema":
			return "missing_archive_chunk"
		}
	}
	var missingChunk *chunks.MissingChunkError
	// A concrete missing-hash / missing-file error at a stored-root or stored
	// table read is distinguishable from corrupt bytes and failed decoding.
	if !errors.Is(err, doltdb.ErrHashNotFound) && !errors.Is(err, fs.ErrNotExist) && !errors.As(err, &missingChunk) {
		return ""
	}
	switch kind {
	case "root", "root-lock", "census":
		return "missing_root_chunk"
	case "table", "rows":
		return "missing_table_chunk"
	case "schema":
		return "unreadable_schema"
	case "resolve":
		return "other"
	}
	return ""
}

type PhysicalHistoryCommit struct {
	ID             string
	Parents        []string
	OrderedParents []string
	Metadata       map[string]driver.Value
}

type PhysicalHistoryBoundary struct {
	CommitID, Reason string
	SQLCodes         []uint16
	Cause            error
}

// The commit itself authenticates the expected root address. Checking the
// backing store distinguishes an absent value from a present corrupt value;
// GetRootValue alone uses an unexported error for the former in the pinned engine.
type storedChunkAbsent struct{ reason string }

func (*storedChunkAbsent) Error() string { return "authenticated stored chunk absent" }

func readPhysicalRoot(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit) (doltdb.RootValue, error) {
	address, err := datas.GetCommitRootHash(commit.Value())
	if err != nil {
		return nil, err
	}
	if address.IsEmpty() {
		return nil, errors.New("commit has no authenticated root address")
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	present, err := store.Has(ctx, address)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, &storedChunkAbsent{reason: "missing_root_chunk"}
	}
	return commit.GetRootValue(ctx)
}

// Retain only detached type names and original numeric codes for diagnostics.
// Translation remains available to SQL consumers, but its generic fallback
// number must never be mistaken for an observed source code.
type physicalErrorEvidence struct {
	cause     error
	types     []string
	codes     []uint16
	innermost string
}

func (*physicalErrorEvidence) Error() string                      { return "physical source error" }
func (e *physicalErrorEvidence) Unwrap() error                    { return e.cause }
func (e *physicalErrorEvidence) SafeGoErrorTypes() []string       { return append([]string{}, e.types...) }
func (e *physicalErrorEvidence) SafeSQLCodes() []uint16           { return append([]uint16{}, e.codes...) }
func (e *physicalErrorEvidence) SafeInnermostGoErrorType() string { return e.innermost }

func physicalInnermostErrorType(err error) string {
	leaf, deepest := "", -1
	var visit func(error, int)
	visit = func(e error, depth int) {
		if e == nil || depth > 32 {
			return
		}
		hasChild := false
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				if child != nil {
					hasChild = true
					visit(child, depth+1)
				}
			}
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				hasChild = true
				visit(child, depth+1)
			}
		}
		if !hasChild && depth > deepest {
			deepest, leaf = depth, reflect.TypeOf(e).String()
		}
	}
	visit(err, 0)
	return leaf
}

func physicalErrorTypes(err error) []string {
	result := []string{}
	seen := map[string]bool{}
	var visit func(error, int)
	visit = func(e error, depth int) {
		if e == nil || depth > 32 {
			return
		}
		name := reflect.TypeOf(e).String()
		if !seen[name] {
			result = append(result, name)
			seen[name] = true
		}
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child, depth+1)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	return result
}

type PhysicalHistoryLog struct {
	Head          string
	ReadOnlyScope bool
	Commits       []PhysicalHistoryCommit
	Boundaries    []PhysicalHistoryBoundary
}

// ReadPhysicalHistory reads actual stored parent edges, including ghost parents
// that SQL ancestry enumeration cannot represent as readable commits. Frontier
// nodes have no invented metadata, root, parent list or image.
func ReadPhysicalHistory(ctx context.Context, conn *sql.Conn, head string) (result PhysicalHistoryLog, resultErr error) {
	if !physicalCommitID.MatchString(head) {
		return result, &PhysicalReadError{Kind: "invalid-commit", CommitID: head, Cause: errors.New("exact HEAD required")}
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok || d.gmsCtx.GetTransaction() != nil {
			return &PhysicalReadError{Kind: "occupied-transaction", CommitID: head, Cause: errors.New("dedicated idle embedded connection required")}
		}
		query, _, rows, err := d.queryWithBindings(ctx, "START TRANSACTION READ ONLY", nil)
		if err != nil {
			return physicalReadError("transaction", head, "", err)
		}
		rollback := doltTx{ctx: d.gmsCtx.Context, conn: d}
		defer func() {
			err := rollback.Rollback()
			if err == nil && d.gmsCtx.GetTransaction() != nil {
				err = errors.New("physical history transaction remains active")
			}
			if err != nil {
				scopeErr = errors.Join(scopeErr, physicalReadError("rollback", head, "", err))
			}
		}()
		if err := rows.Close(query); err != nil {
			return physicalReadError("transaction", head, "", err)
		}
		if transaction := d.gmsCtx.GetTransaction(); transaction == nil || !transaction.IsReadOnly() {
			return &PhysicalReadError{Kind: "transaction", CommitID: head, Cause: errors.New("read-only transaction not authenticated")}
		}
		result, scopeErr = d.readPhysicalHistory(ctx, head)
		if scopeErr == nil {
			result.ReadOnlyScope = true
		}
		return scopeErr
	})
	if resultErr != nil {
		result = PhysicalHistoryLog{}
	}
	return
}

func (d *DoltConn) readPhysicalHistory(ctx context.Context, head string) (PhysicalHistoryLog, error) {
	q, err := d.beginQuery(ctx, "immutable physical history census")
	if err != nil {
		return PhysicalHistoryLog{}, physicalReadError("query", head, "", err)
	}
	defer d.endQuery(q)
	if q.Client().User != "root" {
		return PhysicalHistoryLog{}, physicalReadError("permission", head, "", errors.New("local root required"))
	}
	ddb, ok := dsess.DSessFromSess(q.Session).GetDoltDB(q, q.GetCurrentDatabase())
	if !ok {
		return PhysicalHistoryLog{}, physicalReadError("database", head, "", errors.New("Dolt database unavailable"))
	}
	spec, err := doltdb.NewCommitSpec(head)
	if err != nil {
		return PhysicalHistoryLog{}, physicalReadError("resolve", head, "", err)
	}
	optional, err := ddb.Resolve(q, spec, nil)
	if err != nil {
		return PhysicalHistoryLog{}, physicalReadError("resolve", head, "", err)
	}
	result := PhysicalHistoryLog{Head: head, Commits: []PhysicalHistoryCommit{}, Boundaries: []PhysicalHistoryBoundary{}}
	state := map[string]int{}
	var visit func(*doltdb.OptionalCommit, string) error
	visit = func(optional *doltdb.OptionalCommit, expected string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if optional == nil {
			return errors.New("missing optional commit observation")
		}
		if state[expected] == 1 {
			return errors.New("physical ancestry cycle")
		}
		if state[expected] == 2 {
			return nil
		}
		if len(state) >= 20000 {
			return errors.New("physical ancestry limit")
		}
		state[expected] = 1
		commit, present := optional.ToCommit()
		if !present {
			if expected == head {
				return physicalReadError("resolve", head, "", doltdb.ErrGhostCommitEncountered)
			}
			result.Boundaries = append(result.Boundaries, PhysicalHistoryBoundary{CommitID: expected, Reason: "ghost_parent"})
			state[expected] = 2
			return nil
		}
		actual, err := commit.HashOf()
		if err != nil {
			return physicalReadError("identity", expected, "", err)
		}
		if actual.String() != expected {
			return errors.New("physical ancestry identity differs")
		}
		// Commit metadata and parent edges do not depend on the commit's root.
		// Preserve the actual graph even when a historical root is absent; the
		// own-version image reader authenticates readability and cuts its suffix.
		meta, err := commit.GetCommitMeta(q)
		if err != nil {
			return physicalReadError("metadata", expected, "", err)
		}
		height, err := commit.Height()
		if err != nil {
			return physicalReadError("metadata", expected, "", err)
		}
		parents, err := commit.ParentHashes(q)
		if err != nil {
			return physicalReadError("parents", expected, "", err)
		}
		record := PhysicalHistoryCommit{ID: expected, Parents: []string{}, Metadata: map[string]driver.Value{
			"committer": meta.Committer.Name, "email": meta.Committer.Email, "date": meta.Committer.Date.Time(), "message": meta.Description, "height": height, "signature": meta.Signature,
			"author": meta.Author.Name, "author_email": meta.Author.Email, "author_date": meta.Author.Date.Time(),
		}}
		for i, parent := range parents {
			record.Parents = append(record.Parents, parent.String())
			previous, err := commit.GetParent(q, i)
			if err != nil {
				if reason := missingPhysicalHistory("resolve", err); reason != "" {
					id := parent.String()
					if state[id] == 1 {
						return errors.New("physical ancestry cycle")
					}
					if state[id] != 2 {
						state[id] = 2
						result.Boundaries = append(result.Boundaries, PhysicalHistoryBoundary{CommitID: id, Reason: reason, SQLCodes: physicalHistorySQLCodes(err), Cause: physicalReadError("resolve", id, "", err)})
					}
					continue
				}
				return physicalReadError("parents", parent.String(), "", err)
			}
			if err := visit(previous, parent.String()); err != nil {
				return err
			}
		}
		record.OrderedParents = append([]string{}, record.Parents...)
		sort.Strings(record.Parents)
		result.Commits = append(result.Commits, record)
		state[expected] = 2
		if len(result.Commits) > 10000 {
			return errors.New("physical commit limit")
		}
		return nil
	}
	if err := visit(optional, head); err != nil {
		return PhysicalHistoryLog{}, err
	}
	sort.Slice(result.Commits, func(i, j int) bool { return result.Commits[i].ID < result.Commits[j].ID })
	sort.Slice(result.Boundaries, func(i, j int) bool { return result.Boundaries[i].CommitID < result.Boundaries[j].CommitID })
	return result, nil
}
