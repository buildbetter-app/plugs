package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Use actual guarded commits so current, historical and staged reads exercise
// the same durable key history and transaction overlays as the Writer.
func prefixWrite(t *testing.T, ctx context.Context, s *Store, lease driver.ActiveLease, del, add []driver.DerivedRow) driver.WriteTx {
	t.Helper()
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	keys := map[string]driver.LockKey{}
	key := testKey("pl-prefix")
	encoded, _ := driver.EncodeLockKey(key)
	keys[encoded] = key
	sentinel, err := driver.NewClosureSentinel("pl-prefix")
	if err != nil {
		t.Fatal(err)
	}
	key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
	encoded, _ = driver.EncodeLockKey(key)
	keys[encoded] = key
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: row.Key}}
			encoded, err := driver.EncodeLockKey(key)
			if err != nil {
				t.Fatal(err)
			}
			keys[encoded] = key
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	locks := make([]driver.LockRequest, 0, len(ordered))
	for _, key := range ordered {
		locks = append(locks, driver.LockRequest{Key: keys[key], Mode: driver.LockUpdate})
	}
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedAttribute, del, add); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	return tx
}

func prefixCommit(t *testing.T, ctx context.Context, s *Store, lease driver.ActiveLease, step int, del, add []driver.DerivedRow) {
	t.Helper()
	tx := prefixWrite(t, ctx, s, lease, del, add)
	ref := testRef("pl-prefix")
	states, err := tx.Observe(ctx, []driver.LockKey{testKey(ref.ID)})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(fmt.Sprintf(`{"step":%d}`, step))}); err != nil {
		t.Fatal(err)
	}
	op := model.OpCreate
	if states[0].Present {
		op = model.OpPatch
	}
	keys := make([]driver.LockKey, 0)
	for _, request := range tx.(*transaction).acquired {
		keys = append(keys, request.Key)
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: getMeta(t, ctx, s).Incarnation, Kind: model.ChangesetWrite,
		Principal: model.PrincipalRef{Kind: "system", ID: "prefix-test"}, LockKeys: keys,
		Changes: []model.ChangeRecord{{Ref: ref, Op: op, FromRevision: states[0].Revision}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func requirePrefixRows(t *testing.T, ctx context.Context, r driver.Reader, prefixes []string, want []driver.DerivedRow) {
	t.Helper()
	before := append([]string(nil), prefixes...)
	rows, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, prefixes)
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("prefix rows=%+v want=%+v err=%v", rows, want, err)
	}
	if !reflect.DeepEqual(before, prefixes) {
		t.Fatal("enumeration mutated caller prefixes")
	}
	if len(rows) != 0 {
		rows[0].Value[0] = '!'
		again, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, prefixes)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("caller mutated retained row bytes: rows=%+v err=%v", again, err)
		}
	}
}

func TestDerivedPrefixUnionSnapshotHistoryAndStaging(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "prefixes.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	a := driver.DerivedRow{Key: "a/one", Value: []byte("one")}
	b := driver.DerivedRow{Key: "a/two", Value: []byte("two")}
	c := driver.DerivedRow{Key: "b/three", Value: []byte("three")}
	initial := []driver.DerivedRow{a, b, c, {Key: "ab/outside", Value: []byte("outside")}}
	prefixes := []string{"b/", "a/two", "a/", "a/", "missing/"}
	want := []driver.DerivedRow{a, b, c}
	// More than one SQL batch, with interspersed unselected live rows.
	for i := 0; i < 260; i++ {
		key := fmt.Sprintf("bucket-%03d/value", i)
		row := driver.DerivedRow{Key: key, Value: []byte(key)}
		initial = append(initial, row)
		if i%2 == 0 || i >= 250 {
			prefixes = append(prefixes, fmt.Sprintf("bucket-%03d/", i))
			want = append(want, row)
		}
	}
	prefixCommit(t, ctx, s, lease, 1, nil, initial)
	m := getMeta(t, ctx, s)
	pinned := getReader(t, ctx, s, m, nil)
	updated := driver.DerivedRow{Key: b.Key, Value: []byte("updated")}
	added := driver.DerivedRow{Key: "a/four", Value: []byte("four")}
	prefixCommit(t, ctx, s, lease, 2, []driver.DerivedRow{a}, []driver.DerivedRow{updated, added})
	requirePrefixRows(t, ctx, pinned, prefixes, want)
	at := model.Natural64(1)
	m = getMeta(t, ctx, s)
	historical := getReader(t, ctx, s, m, &at)
	requirePrefixRows(t, ctx, historical, prefixes, want)
	current := getReader(t, ctx, s, m, nil)
	now := append([]driver.DerivedRow{added, updated, c}, want[3:]...)
	requirePrefixRows(t, ctx, current, prefixes, now)
	requirePrefixRows(t, ctx, current, nil, []driver.DerivedRow{})
	all, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, []string{"a/", "", "bucket-000/"})
	if err != nil || len(all) != len(initial) {
		t.Fatalf("whole-table prefix union rows=%d err=%v", len(all), err)
	}
	other, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{""})
	if err != nil || len(other) != 0 {
		t.Fatalf("table isolation rows=%+v err=%v", other, err)
	}
	staged := driver.DerivedRow{Key: "a/staged", Value: []byte("staged")}
	replaced := driver.DerivedRow{Key: b.Key, Value: []byte("staged-two")}
	tx := prefixWrite(t, ctx, s, lease, []driver.DerivedRow{c}, []driver.DerivedRow{staged, replaced})
	requirePrefixRows(t, ctx, tx, []string{"b/", "a/", "a/staged"}, []driver.DerivedRow{added, staged, replaced})
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	requirePrefixRows(t, ctx, current, prefixes, now)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, prefixes); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("closed reader: %v", err)
	}
}

func TestDerivedPrefixReadRefusals(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "refusals.db"))
	r := getReader(t, ctx, s, getMeta(t, ctx, s), nil).(driver.DerivedPrefixReader)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.DerivedRowsForPrefixes(canceled, driver.DerivedAttribute, []string{"a/"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := r.DerivedRowsForPrefixes(ctx, "invalid", nil); err == nil {
		t.Fatal("accepted unknown table for empty input")
	}
	if _, err := r.DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, []string{"", "bad\x00prefix"}); err == nil {
		t.Fatal("ignored invalid prefix hidden by the whole-table prefix")
	}
}

func TestDerivedPrefixReadReportsMalformedHistory(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "malformed.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	row := driver.DerivedRow{Key: "a/one", Value: []byte("one")}
	prefixCommit(t, ctx, s, lease, 1, nil, []driver.DerivedRow{row})
	key := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: row.Key}})
	if _, err := s.db.ExecContext(ctx, `UPDATE key_history SET data=? WHERE key=?`, []byte("{"), key); err != nil {
		t.Fatal(err)
	}
	r := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	if rows, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAttribute, []string{"a/"}); err == nil {
		t.Fatalf("malformed native history became successful rows: %+v", rows)
	}
}
