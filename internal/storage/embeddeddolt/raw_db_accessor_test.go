//go:build cgo

package embeddeddolt_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
)

func TestRawDBTxAccessorScopesEmbeddedConnection(t *testing.T) {
	env := newTestEnv(t, "rawtx")
	ctx := context.Background()
	accessor, ok := any(env.store).(interface {
		WithRawDBTx(context.Context, *sql.TxOptions, func(*sql.Tx) error) error
	})
	if !ok {
		t.Fatal("embedded store does not expose transaction-scoped raw access")
	}
	if _, ok := any(env.store).(storage.RawDBAccessor); ok {
		t.Fatal("embedded store must not expose a long-lived raw database")
	}

	opts := &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead}
	var callbackTx *sql.Tx
	if err := accessor.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
		callbackTx = tx
		var count int
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues").Scan(&count)
	}); err != nil {
		t.Fatalf("WithRawDBTx: %v", err)
	}
	if callbackTx == nil {
		t.Fatal("WithRawDBTx did not invoke its callback")
	}
	if _, err := callbackTx.ExecContext(ctx, "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("callback transaction remains usable after return: %v", err)
	}

	const callbackErr = "callback stopped"
	err := accessor.WithRawDBTx(ctx, opts, func(*sql.Tx) error {
		return errors.New(callbackErr)
	})
	if err == nil || err.Error() != callbackErr {
		t.Fatalf("WithRawDBTx callback error = %v, want %q", err, callbackErr)
	}
	if err := accessor.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues").Err()
	}); err != nil {
		t.Fatalf("WithRawDBTx after callback error: %v", err)
	}
}
