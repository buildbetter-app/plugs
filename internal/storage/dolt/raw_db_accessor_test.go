package dolt

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRawDBTxAccessorScopesServerConnection(t *testing.T) {
	ctx := context.Background()
	opts := &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &DoltStore{db: db}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	mock.ExpectRollback()
	var callbackTx *sql.Tx
	if err := s.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
		callbackTx = tx
		var value int
		return tx.QueryRowContext(ctx, "SELECT 1").Scan(&value)
	}); err != nil {
		t.Fatal(err)
	}
	if callbackTx == nil {
		t.Fatal("raw scope did not invoke its callback")
	}
	if _, err := callbackTx.ExecContext(ctx, "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("callback transaction remains usable after return: %v", err)
	}

	// The callback can have external effects, so even a retryable SQL error
	// must not cause the store to invoke it a second time.
	callbackErr := errors.New("invalid connection")
	mock.ExpectBegin()
	mock.ExpectRollback()
	calls := 0
	if err := s.WithRawDBTx(ctx, opts, func(*sql.Tx) error {
		calls++
		return callbackErr
	}); !errors.Is(err, callbackErr) || calls != 1 {
		t.Fatalf("callback error = %v, calls = %d", err, calls)
	}

	beginErr := errors.New("begin refused")
	mock.ExpectBegin().WillReturnError(beginErr)
	if err := s.WithRawDBTx(ctx, opts, func(*sql.Tx) error {
		t.Error("failed begin invoked callback")
		return nil
	}); !errors.Is(err, beginErr) {
		t.Fatalf("begin error = %v", err)
	}

	rollbackErr := errors.New("rollback failed")
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(rollbackErr)
	if err := s.WithRawDBTx(ctx, opts, func(*sql.Tx) error { return callbackErr }); !errors.Is(err, callbackErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("callback and rollback errors = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectRollback()
	func() {
		defer func() {
			if got := recover(); got != "callback panic" {
				t.Errorf("panic = %v", got)
			}
		}()
		_ = s.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
			callbackTx = tx
			panic("callback panic")
		})
	}()
	if _, err := callbackTx.ExecContext(ctx, "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("panicking callback left a usable transaction: %v", err)
	}
	s.closed.Store(true)
	if err := s.WithRawDBTx(ctx, opts, func(*sql.Tx) error {
		t.Error("closed store invoked callback")
		return nil
	}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed scope error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
