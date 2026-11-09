package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	selectionRowInsertMaxRows     = 128
	selectionRowInsertMaxBindings = 999
)

type selectionRowInsertKind uint8

const (
	selectionRowInsertBead selectionRowInsertKind = iota
	selectionRowInsertFactSet
	selectionRowInsertFact
)

func (kind selectionRowInsertKind) layout() (string, int) {
	switch kind {
	case selectionRowInsertBead:
		return `INSERT INTO selection_bead_history(
		 incarnation,branch,derived_key,record_key,start_revision,bead_id,bead_type,project,status,title,template,parent_key,priority,membership_leaf,revision_text,source_revision,source_digest) VALUES `, 17
	case selectionRowInsertFactSet:
		return `INSERT INTO selection_fact_sets(
		 incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest) VALUES `, 10
	default: // only AddFact constructs the remaining private kind
		return `INSERT INTO selection_facts(
		 incarnation,branch,row_key,start_revision,ordinal,kind,ref_key,target_key,link_kind,subject,scope,role,scope_kind,project_name,name,value,active,depth) VALUES `, 18
	}
}

type selectionRowInsertShape struct {
	kind selectionRowInsertKind
	rows int
}

// selectionRowInsertBatch coalesces only consecutive inserts of the same SQL.
// A table switch flushes its predecessor: a set is inserted before its facts,
// and no row crosses another source's intervening statement. The caller must
// Flush before later fallible Go work or coverage publication, and Close before
// committing. Close deliberately discards rather than publishes pending rows.
//
// Statement-wide ABORT and immediate FK checks have a different in-transaction
// prefix/error boundary from scalar statements. Arbitrary triggers observing
// changes() also observe the new statement boundaries. Outer rollback does not
// establish equivalence for either property; callers requiring them cannot use
// multi-row coalescing. No failed batch is retried as scalar writes.
type selectionRowInsertBatch struct {
	conn       *sql.Conn
	kind       selectionRowInsertKind
	rows       int
	values     []any
	statements map[selectionRowInsertShape]*sql.Stmt
	firstUse   []*sql.Stmt
	err        error
	closed     bool
	closeErr   error
}

func newSelectionRowInsertBatch(conn *sql.Conn) *selectionRowInsertBatch {
	return &selectionRowInsertBatch{conn: conn}
}

func (b *selectionRowInsertBatch) AddBead(ctx context.Context, values ...any) error {
	return b.add(ctx, selectionRowInsertBead, values)
}

func (b *selectionRowInsertBatch) AddFactSet(ctx context.Context, values ...any) error {
	return b.add(ctx, selectionRowInsertFactSet, values)
}

func (b *selectionRowInsertBatch) AddFact(ctx context.Context, values ...any) error {
	return b.add(ctx, selectionRowInsertFact, values)
}

func (b *selectionRowInsertBatch) add(ctx context.Context, kind selectionRowInsertKind, values []any) error {
	if err := b.check(); err != nil {
		return err
	}
	_, columns := kind.layout()
	if b.rows != 0 && (b.kind != kind || len(values) != columns) {
		if err := b.Flush(ctx); err != nil {
			return err
		}
	}
	if len(values) != columns {
		b.err = fmt.Errorf("sqlite: selection row insert has %d values, want %d", len(values), columns)
		return b.err
	}
	if err := ctx.Err(); err != nil {
		b.err = err
		return err
	}
	b.kind = kind
	for _, value := range values {
		// The existing caller supplies scalar values. Also detach byte values
		// so later buffer reuse cannot change an already accepted SQL argument.
		if bytes, ok := value.([]byte); ok && bytes != nil {
			copied := make([]byte, len(bytes))
			copy(copied, bytes)
			value = copied
		}
		b.values = append(b.values, value)
	}
	b.rows++
	if b.rows == min(selectionRowInsertMaxRows, selectionRowInsertMaxBindings/columns) {
		return b.Flush(ctx)
	}
	return nil
}

func (b *selectionRowInsertBatch) check() error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return errors.New("sqlite: selection row insert batch is closed")
	}
	return nil
}

func (b *selectionRowInsertBatch) Flush(ctx context.Context) error {
	if err := b.check(); err != nil {
		return err
	}
	if b.rows == 0 {
		return nil
	}
	shape := selectionRowInsertShape{kind: b.kind, rows: b.rows}
	statement := b.statements[shape]
	if statement == nil {
		prefix, columns := b.kind.layout()
		row := "(" + strings.TrimSuffix(strings.Repeat("?,", columns), ",") + ")"
		query := prefix + strings.TrimSuffix(strings.Repeat(row+",", b.rows), ",")
		var err error
		statement, err = b.conn.PrepareContext(ctx, query)
		if err != nil {
			b.err = err
			return err
		}
		if b.statements == nil {
			b.statements = make(map[selectionRowInsertShape]*sql.Stmt)
		}
		b.statements[shape] = statement
		b.firstUse = append(b.firstUse, statement)
	}
	if _, err := statement.ExecContext(ctx, b.values...); err != nil {
		b.err = err
		return err
	}
	clear(b.values)
	b.values = b.values[:0]
	b.rows = 0
	return nil
}

// Close is idempotent and attempts every prepared handle in first-use order.
// The caller preserves an earlier Add/Flush/validation error over this cleanup
// result. Closing is never permission to flush or commit an incomplete group.
func (b *selectionRowInsertBatch) Close() error {
	if b.closed {
		return b.closeErr
	}
	b.closed = true
	for _, statement := range b.firstUse {
		if err := statement.Close(); err != nil && b.closeErr == nil {
			b.closeErr = err
		}
	}
	b.firstUse = nil
	b.statements = nil
	clear(b.values)
	b.values = nil
	b.rows = 0
	return b.closeErr
}
