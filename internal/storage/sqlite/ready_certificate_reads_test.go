package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReadyOrderLockReturnCannotAlterAcquiredBeforeImage(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "detached-ready.db"))
	t.Cleanup(func() { _ = s.Close() })
	f := readyorder.NewCertificateFixture(t, s)
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PrepareReadyProjection(ctx, r, nil, nil, nil)
	closeErr := r.Close()
	if err != nil || closeErr != nil || !plan.Available() {
		t.Fatalf("real Ready control preparation: plan=%v err=%v close=%v", plan, err, closeErr)
	}
	keys := plan.LockKeys()
	requests := make(driver.LockSet, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	writer, err := s.Write(ctx, f.Lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := writer.(*transaction)
	defer tx.Close()
	states, err := tx.Lock(ctx, requests)
	if err != nil {
		t.Fatal(err)
	}
	var originalKey driver.LockKey
	var originalValue []byte
	var altered driver.LockState
	mutated := false
	for i := range states {
		state := &states[i]
		if state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedReadyState || !state.Present || state.Derived == nil || len(state.Derived.Value) == 0 {
			continue
		}
		originalKey = copyKey(state.Key)
		originalValue = bytes.Clone(state.Derived.Value)
		state.Key.Derived.Key = "caller-edited-lock-key"
		state.Derived.Key = "caller-edited-row-key"
		state.Derived.Value[0] ^= 0xff
		state.Derived.Index = &driver.SelectionIndex{}
		altered = *state
		mutated = true
		break
	}
	if !mutated {
		t.Fatal("real Ready publication did not observe a present physical row")
	}
	retained, ok := tx.readyOrderAcquiredStates([]driver.LockKey{originalKey})
	state := retained[keyID(originalKey)]
	if !ok || state.Key.Derived == nil || state.Key.Derived.Key != originalKey.Derived.Key || state.Derived == nil || state.Derived.Key != originalKey.Derived.Key || state.Derived.Index != nil || !bytes.Equal(state.Derived.Value, originalValue) {
		t.Fatalf("returned Lock state altered retained Ready before-image: ok=%t state=%+v", ok, state)
	}
	if readyOrderSameImage(state, altered, true) {
		t.Fatal("caller-altered Lock result matched the physical before-image")
	}
	if head, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	} else {
		n, _ := head.Uint64()
		if _, err = plan.Publish(ctx, tx, model.Natural64(n+1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := plan.Finalize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "ready-cache-test"}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	result, err := f.Select(ctx)
	if err != nil || result.Total != 1 || len(result.Keys) != 1 || result.Keys[0].Ref.ID != f.Ref.ID {
		t.Fatalf("caller mutation discarded genuine Ready proof: %+v %v", result, err)
	}
}

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
