package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Forward real native reads, consuming cancellation only at this helper's
// actual query boundary. Counts measure dispatch, not executor row visits.
type readyPhysicalConnector struct{ *selectionStageConnector }

func (c *readyPhysicalConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &readyPhysicalConn{&selectionStageConn{Conn: conn, counts: c.selectionStageConnector}}, nil
}

type readyPhysicalConn struct{ *selectionStageConn }

func (c *readyPhysicalConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	matched := strings.Contains(query, "WITH requested(key) AS (VALUES")
	var cancel context.CancelFunc
	if matched {
		c.counts.mu.Lock()
		cancel, c.counts.cancel = c.counts.cancel, nil
		c.counts.mu.Unlock()
	}
	if cancel != nil {
		c.counts.cancelTriggers.Add(1)
		cancel()
		c.counts.nativeAttempts.Add(1)
	}
	rows, err := c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, args)
	if matched && err == nil {
		c.counts.queries.Add(1)
	}
	if cancel != nil && errors.Is(err, context.Canceled) {
		c.counts.canceledNative.Add(1)
	}
	return rows, err
}

func TestReadyOrderPhysicalReadBatchesLatestImages(t *testing.T) {
	ctx := testContext(t)
	s, counts := selectionStageStore(t, ctx)
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.db = sql.OpenDB(&readyPhysicalConnector{counts})
	c, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	keys := make([]driver.LockKey, 2*readyOrderPhysicalReadBatch+3)
	for i := range keys {
		rowKey := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("proof-%03d", i)}
		keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey}
		if i%5 == 0 {
			continue // a requested physical miss must remain explicit
		}
		for _, revision := range []uint64{1, 2} {
			state := driver.LockState{Key: keys[i], Revision: model.Natural64(revision), Present: i%7 != 0}
			if state.Present {
				state.Derived = &driver.DerivedRow{Key: rowKey.Key, Value: []byte(fmt.Sprintf("image-%d-%d", i, revision))}
			}
			encoded, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(keys[i]), revKey(revision), encoded); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Retained versions must not change which one exact key lookup returns.
	for revision := uint64(3); revision <= 1002; revision++ {
		state := driver.LockState{Key: keys[1], Revision: model.Natural64(revision), Present: true,
			Derived: &driver.DerivedRow{Key: keys[1].Derived.Key, Value: []byte(fmt.Sprintf("deep-%d", revision))}}
		encoded, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(keys[1]), revKey(revision), encoded); err != nil {
			t.Fatal(err)
		}
	}
	counts.queries.Store(0)
	actual, err := readyOrderReadStates(ctx, c, keys)
	if err != nil || len(actual) != len(keys) || counts.queries.Load() != 3 {
		t.Fatalf("bounded native reads: states=%d queries=%d err=%v", len(actual), counts.queries.Load(), err)
	}
	for _, key := range keys {
		want, err := readyOrderReadState(ctx, c, key)
		if err != nil {
			t.Fatal(err)
		}
		wantJSON, _ := json.Marshal(want)
		actualJSON, _ := json.Marshal(actual[keyID(key)])
		if !bytes.Equal(wantJSON, actualJSON) {
			t.Fatalf("batch differs from native latest point read for %s", keyID(key))
		}
	}
	if _, err = c.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(ctx, `UPDATE key_history SET data=? WHERE key=? AND rev=?`, []byte("corrupt"), keyID(keys[1]), revKey(1002)); err != nil {
		t.Fatal(err)
	}
	if _, err = readyOrderReadStates(ctx, c, keys); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("corrupt actual latest image admitted: %v", err)
	}
	if _, err = c.ExecContext(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if _, err = readyOrderReadStates(ctx, c, keys); err != nil {
		t.Fatal("rollback did not restore exact latest images", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	counts.mu.Lock()
	counts.cancel = cancel
	counts.mu.Unlock()
	if _, err = readyOrderReadStates(canceled, c, keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("native cancellation did not propagate: %v", err)
	}
	if counts.cancelTriggers.Load() != 1 || counts.nativeAttempts.Load() != 1 || counts.canceledNative.Load() != 1 {
		t.Fatalf("native cancellation not reached: trigger=%d attempt=%d actual=%d", counts.cancelTriggers.Load(), counts.nativeAttempts.Load(), counts.canceledNative.Load())
	}
}

func TestReadyOrderPhysicalReadBatchesCurrentSources(t *testing.T) {
	ctx := testContext(t)
	s, _ := selectionStageStore(t, ctx)
	c, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	keys := make([]string, readyOrderPhysicalReadBatch+1)
	want := map[string][]byte{}
	for i := range keys {
		ref := model.RefByID(fmt.Sprintf("source-%03d", i), model.Natural64(0))
		keys[i], err = recordKey(ref)
		if err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			continue
		}
		record := driver.Record{Ref: ref, Kind: "bead", Revision: model.Natural64(9), Body: []byte(fmt.Sprintf("source body %d", i))}
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?)`, keys[i], encoded); err != nil {
			t.Fatal(err)
		}
		want[keys[i]] = encoded
	}
	actual, err := readyOrderReadPhysical(ctx, c, keys, false)
	if err != nil || len(actual) != len(keys) {
		t.Fatalf("source image reads: %d %v", len(actual), err)
	}
	for _, key := range keys {
		encoded, present := want[key]
		if actual[key].present != present || !bytes.Equal(actual[key].data, encoded) {
			t.Fatalf("source presence/body changed at %s", key)
		}
	}
}
