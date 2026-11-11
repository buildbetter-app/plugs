package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/fs"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
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
	// A concrete missing-hash / missing-file error at a stored-root or stored
	// table read is distinguishable from corrupt bytes and failed decoding.
	if !errors.Is(err, doltdb.ErrHashNotFound) && !errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	switch kind {
	case "root", "root-lock", "census":
		return "missing_root_chunk"
	case "table", "rows":
		return "missing_table_chunk"
	case "schema":
		return "unreadable_schema"
	}
	return ""
}

type PhysicalHistoryCommit struct {
	ID       string
	Parents  []string
	Metadata map[string]driver.Value
}

type PhysicalHistoryBoundary struct {
	CommitID, Reason string
	SQLCodes         []uint16
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
		if _, err := commit.GetRootValue(q); err != nil {
			if reason := missingPhysicalHistory("root", err); reason != "" && expected != head {
				result.Boundaries = append(result.Boundaries, PhysicalHistoryBoundary{CommitID: expected, Reason: reason, SQLCodes: physicalHistorySQLCodes(err)})
				state[expected] = 2
				return nil
			}
			return physicalReadError("root", expected, "", err)
		}
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
				return physicalReadError("parents", expected, "", err)
			}
			if err := visit(previous, parent.String()); err != nil {
				return err
			}
		}
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
