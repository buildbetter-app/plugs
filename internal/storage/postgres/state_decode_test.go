package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestObservedStateDecodeMatchesOriginalJSON(t *testing.T) {
	selected := model.RefByID("opaque\xff", model.Natural64(0))
	unselected := model.Ref{ID: selected.ID, Branch: selected.Branch}
	emptyBranch := model.RefByID(selected.ID, "")
	keys := []driver.LockKey{
		{Kind: driver.LockSentinel, Sentinel: "opaque\x00\xff"},
		{Kind: driver.LockReadFact, Fact: "predicate\xff"},
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "opaque\x00\xff"}},
		{Kind: driver.LockRecord, Ref: &selected},
		{Kind: driver.LockRecord, Ref: &unselected},
		{Kind: driver.LockRecord, Ref: &emptyBranch},
		{},
	}
	cases := []struct{ name, raw string }{
		{"empty_object", `{}`},
		{"null", `null`},
		{"partial_key", `{"Key":{"Fact":"replacement"},"Present":true}`},
		{"null_key", `{"Key":null,"Revision":"9"}`},
		{"empty_bytes", `{"Derived":{"Key":"row","Value":""},"Record":{"Body":""}}`},
		{"null_bytes", `{"Derived":{"Key":"row","Value":null},"Record":{"Body":null}}`},
		{"record", `{"Record":{"Ref":{"id":"pl-r","branch":"0"},"Kind":"test","Body":"AAH/"},"Revision":"7","Present":true}`},
		{"derived", `{"Derived":{"Key":"row","Value":"AAH/"},"Revision":"7","Present":true}`},
		{"inline_empty_slices", `{"Derived":{"Key":"row","Index":{"Queue":{"Links":[]},"Bead":{"Absent":[]},"Facts":{"Rows":[]}}}}`},
		{"unknown_and_trailing", `{"Unknown":123} {"ignored":true}`},
		{"bad_json", `{"Present":`},
		{"bad_revision", `{"Revision":-1}`},
		{"bad_ref", `{"Key":{"Ref":{"id":"pl-r","key":"both"}}}`},
	}
	r := &reader{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i, key := range keys {
				want := driver.LockState{Key: copyLockKey(key), Revision: model.Natural64(0)}
				wantErr := decode([]byte(tc.raw), &want)
				for repeat := 0; repeat < 2; repeat++ {
					before := len(r.stateDecodeCache)
					got, err := r.decodeObservedState([]byte(tc.raw), key)
					if (err == nil) != (wantErr == nil) || (err != nil && (reflect.TypeOf(err) != reflect.TypeOf(wantErr) || err.Error() != wantErr.Error())) {
						t.Fatalf("key %d repeat %d error=%v want=%v", i, repeat, err, wantErr)
					}
					if err != nil {
						if len(r.stateDecodeCache) != before {
							t.Fatal("failed decode retained")
						}
						continue
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("key %d repeat %d result=%#v want=%#v", i, repeat, got, want)
					}
					if want.Derived != nil && want.Derived.Index != nil && len(r.stateDecodeCache) != before {
						t.Fatal("inline annotation did not retain original decoding path")
					}
					if got.Key.Ref != nil {
						got.Key.Ref.ID = "caller-mutation"
					}
					if got.Key.Derived != nil {
						got.Key.Derived.Key = "caller-mutation"
					}
					if got.Record != nil && len(got.Record.Body) > 0 {
						got.Record.Body[0] ^= 0xff
					}
					if got.Derived != nil {
						got.Derived.Key = "caller-mutation"
						if len(got.Derived.Value) > 0 {
							got.Derived.Value[0] ^= 0xff
						}
					}
				}
			}
		})
	}
}

func TestObservedStateDecodeRetentionIsBounded(t *testing.T) {
	r := &reader{}
	for i := 0; i <= observedStateDecodeMaxEntries; i++ {
		key := driver.LockKey{Kind: driver.LockReadFact, Fact: driver.ReadFact(fmt.Sprintf("fact/%d", i))}
		if _, err := r.decodeObservedState([]byte(`{}`), key); err != nil {
			t.Fatal(err)
		}
		if len(r.stateDecodeCache) > observedStateDecodeMaxEntries || r.stateDecodeBytes > observedStateDecodeMaxBytes {
			t.Fatal("decode retention exceeded bounds")
		}
	}
	before := len(r.stateDecodeCache)
	raw := []byte(`{"ignored":"` + strings.Repeat("x", observedStateDecodeMaxBytes) + `"}`)
	key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: "oversized"}
	want := driver.LockState{Key: key, Revision: model.Natural64(0)}
	if err := decode(raw, &want); err != nil {
		t.Fatal(err)
	}
	got, err := r.decodeObservedState(raw, key)
	if err != nil || !reflect.DeepEqual(got, want) || len(r.stateDecodeCache) != before {
		t.Fatalf("oversized original decode changed: %v", err)
	}
}

func TestObservedStateReuseRetainsRealSQLAndCurrentOverlay(t *testing.T) {
	ctx := context.Background()
	s, lease := openCommitQueryTest(t, prefixQueryTracer{})
	rows := make([]driver.DerivedRow, 64)
	keys := make([]driver.LockKey, len(rows))
	for i := range rows {
		rows[i] = driver.DerivedRow{Key: fmt.Sprintf("decode/%03d", i), Value: []byte{byte(i), 0, 0xff}}
		keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: rows[i].Key}}
	}
	revision := pgPrefixCommit(t, s, lease, nil, rows)
	pinned := testRead(t, s, nil)
	tx, _ := beginTest(t, s, lease, keys...)
	writer := tx.(*transaction)
	read := func() []driver.LockState {
		t.Helper()
		capture := &prefixQueryCapture{}
		states, err := tx.Observe(context.WithValue(ctx, prefixQueryCaptureKey{}, capture), keys)
		if err != nil || len(states) != len(keys) {
			t.Fatalf("real Observe states=%d: %v", len(states), err)
		}
		capture.mu.Lock()
		defer capture.mu.Unlock()
		if len(capture.events) != 1 || !capture.events[0].returned || capture.events[0].err != nil || capture.events[0].rows != int64(len(keys)) {
			t.Fatalf("fresh actual states SQL events=%+v", capture.events)
		}
		return states
	}
	want := read()
	if len(writer.stateDecodeCache) != len(keys) {
		t.Fatalf("real current rows retained=%d want=%d", len(writer.stateDecodeCache), len(keys))
	}
	for repeat := 0; repeat < 3; repeat++ {
		got := read()
		if !reflect.DeepEqual(got, want) {
			t.Fatal("repeated SQL changed the full observation")
		}
		got[0].Key.Derived.Key = "caller-mutation"
		got[0].Derived.Value[0] ^= 0xff
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.Observe(cancelled, keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached state bypassed cancellation: %v", err)
	}
	id, err := keyID(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	replacement := cloneObservedState(want[0])
	replacement.Derived.Value = []byte("same-token replacement")
	raw, err := encode(replacement)
	if err != nil {
		t.Fatal(err)
	}
	update := func(value []byte) {
		t.Helper()
		result, err := writer.sql.ExecContext(ctx, `UPDATE plug_postgres.states SET value=$2 WHERE key=$1 AND revision=$3`, []byte(id), value, revision.String())
		if err != nil {
			t.Fatal(err)
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			t.Fatalf("same-token replacement affected=%d: %v", n, err)
		}
	}
	update(raw)
	if got := read()[0]; !reflect.DeepEqual(got, replacement) {
		t.Fatalf("stale decode after same-token SQL replacement: %+v", got)
	}
	update([]byte("malformed JSON"))
	if _, err := tx.Observe(ctx, keys); err == nil {
		t.Fatal("cached state hid malformed current SQL bytes")
	}
	update(raw)
	staged := driver.DerivedRow{Key: rows[0].Key, Value: []byte("staged after cached observation")}
	if err := tx.PutDerived(ctx, driver.DerivedAttribute, nil, []driver.DerivedRow{staged}); err != nil {
		t.Fatal(err)
	}
	if got := read()[0]; got.Revision != revision || !got.Present || !reflect.DeepEqual(*got.Derived, staged) {
		t.Fatalf("current overlay lost behind decoded state: %+v", got)
	}
	if err := tx.PutDerived(ctx, driver.DerivedAttribute, []driver.DerivedRow{staged}, nil); err != nil {
		t.Fatal(err)
	}
	if got := read()[0]; got.Revision != revision || got.Present || got.Derived != nil {
		t.Fatalf("staged deletion lost behind decoded state: %+v", got)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if len(writer.stateDecodeCache) != 0 || writer.stateDecodeBytes != 0 {
		t.Fatal("closed reader retained decoded SQL states")
	}
	if _, err := tx.Observe(ctx, keys); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("closed reader reused state: %v", err)
	}
	for _, r := range []driver.Reader{pinned, testRead(t, s, nil)} {
		got, err := r.Observe(ctx, keys)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("pinned/rollback durable image changed: %v", err)
		}
		if !bytes.Equal(got[0].Derived.Value, rows[0].Value) {
			t.Fatal("original opaque payload changed")
		}
	}
}
