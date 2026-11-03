package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func beginDerivedPrefixTest(t *testing.T, s *Store, lease driver.ActiveLease, rows []driver.DerivedRow) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	keys := make([]driver.LockKey, 0, len(rows)+1)
	sentinel, err := driver.NewClosureSentinel("derived-prefix")
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	for _, row := range rows {
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}})
	}
	tx, err := s.Write(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err = tx.Lock(context.Background(), requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = tx.PutDerived(context.Background(), driver.DerivedAncestorClosure, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpSentinels(context.Background(), []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	return tx.(*transaction), driver.ChangesetHeader{Incarnation: s.state.meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "derived-prefix"}, LockKeys: keys}
}

func commitDerivedPrefixTest(t *testing.T, tx *transaction, header driver.ChangesetHeader) model.Changeset {
	t.Helper()
	entry, err := tx.Commit(context.Background(), header)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestDerivedPrefixReaderUsesOnePinnedUnionAndStagedView(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	initial := make([]driver.DerivedRow, 0, 600)
	for i := 0; i < 600; i++ {
		prefix := "b/"
		if i%3 == 0 {
			prefix = "a/"
		}
		initial = append(initial, driver.DerivedRow{Key: fmt.Sprintf("%s%03d", prefix, i), Value: []byte(fmt.Sprintf("value-%03d", i))})
	}
	tx, header := beginDerivedPrefixTest(t, s, lease, initial)
	first := commitDerivedPrefixTest(t, tx, header)
	reader, err := s.Read(ctx, driver.View{Incarnation: s.state.meta.Incarnation, At: &first.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	prefixes := []string{"b/", "a/", "a/", "a/00"}
	want := make([]driver.DerivedRow, 0, len(initial))
	for _, row := range initial {
		if bytes.HasPrefix([]byte(row.Key), []byte("a/")) || bytes.HasPrefix([]byte(row.Key), []byte("b/")) {
			want = append(want, row)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
	before := append([]string(nil), prefixes...)
	got, err := reader.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes)
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(prefixes, before) {
		t.Fatalf("union rows=%d err=%v mutated=%v", len(got), err, !reflect.DeepEqual(prefixes, before))
	}
	got[0].Value[0] = '!'
	again, err := reader.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"a/"})
	if err != nil || bytes.Equal(got[0].Value, again[0].Value) {
		t.Fatal("returned derived bytes alias storage")
	}
	updated := driver.DerivedRow{Key: "a/000", Value: []byte("staged")}
	created := driver.DerivedRow{Key: "a/staged", Value: []byte("created")}
	tx, header = beginDerivedPrefixTest(t, s, lease, []driver.DerivedRow{updated, created, {Key: "b/001"}})
	if err = tx.PutDerived(ctx, driver.DerivedAncestorClosure, []driver.DerivedRow{{Key: "b/001"}}, []driver.DerivedRow{updated, created}); err != nil {
		t.Fatal(err)
	}
	staged, err := tx.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"a/", "b/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != len(want)-1+1 || staged[0].Key != "a/000" || string(staged[0].Value) != "staged" {
		t.Fatalf("staged union=%+v", staged[:min(4, len(staged))])
	}
	commitDerivedPrefixTest(t, tx, header)
	historical, err := reader.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{""})
	if err != nil || !reflect.DeepEqual(historical, want) {
		t.Fatalf("pinned historical rows changed: rows=%d err=%v", len(historical), err)
	}
	current, err := s.Read(ctx, driver.View{Incarnation: s.state.meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	committed, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{""})
	if err != nil || !reflect.DeepEqual(committed, staged) {
		t.Fatalf("committed/staged mismatch: rows=%d err=%v", len(committed), err)
	}
	empty, err := reader.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty input rows=%d err=%v", len(empty), err)
	}
}

func TestDerivedPrefixReaderRefusesMalformedAndClosedReads(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	r, err := s.Read(ctx, driver.View{Incarnation: s.state.meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	prefix := r.(driver.DerivedPrefixReader)
	if _, err = prefix.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"bad\x00"}); err == nil {
		t.Fatal("invalid prefix accepted")
	}
	if _, err = prefix.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, nil); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = prefix.DerivedRowsForPrefixes(cancelled, driver.DerivedAncestorClosure, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	if _, err = prefix.DerivedRowsForPrefixes(ctx, driver.DerivedTable("unknown"), nil); err == nil {
		t.Fatal("unknown table accepted")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = prefix.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"a/"}); err == nil {
		t.Fatal("closed prefix reader accepted")
	}
	if !errors.Is(err, driver.ErrTxClosed) && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed error=%v", err)
	}
}

func TestDerivedPrefixReaderSeeksOnlySelectedHistories(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	rows := make([]driver.DerivedRow, 0, 25000)
	prefixes := make([]string, 5000)
	for i := 24999; i >= 0; i-- {
		key := fmt.Sprintf("other/%05d", i)
		if i < len(prefixes) {
			prefixes[i] = fmt.Sprintf("desc/%05d/", i)
			key = prefixes[i] + "root"
		}
		rows = append(rows, driver.DerivedRow{Key: key, Value: []byte("{malformed-opaque-closure")})
	}
	tx, header := beginDerivedPrefixTest(t, s, lease, rows)
	commitDerivedPrefixTest(t, tx, header)
	work := &derivedPrefixWork{}
	at, _ := s.state.meta.Head.Uint64()
	got, err := s.state.derivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes, at, nil, work)
	if err != nil || len(got) != len(prefixes) || work.histories != len(prefixes) {
		t.Fatalf("selected=%d histories=%d err=%v", len(got), work.histories, err)
	}
	if string(got[0].Value) != "{malformed-opaque-closure" {
		t.Fatal("opaque malformed value changed")
	}
	// Corrupt an unrelated image in a private fork. Selected prefix reads
	// must not inspect it, while a request selecting it must refuse.
	view := s.state.fork()
	bad := "other/24999"
	id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: bad}})
	history := append([]driver.LockState(nil), view.keyHistory[id]...)
	history[len(history)-1].Derived = nil
	view.keyHistory[id] = history
	if _, err = view.derivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes, at, nil, nil); err != nil {
		t.Fatalf("unselected malformed image touched: %v", err)
	}
	if rows, err = view.derivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{bad}, at, nil, nil); err == nil || rows != nil {
		t.Fatalf("selected malformed image returned partial rows=%d err=%v", len(rows), err)
	}
	t.Logf("actual descending publication=25000 keys; requested=5000 prefixes; resolved histories=%d", work.histories)
}

func TestDerivedPrefixReaderPreservesOpaqueKeysAndStagedDeletion(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	keys := []string{"raw/\xff/end", "raw/\xfe/end", "raw/\ufffd/end"}
	rows := make([]driver.DerivedRow, len(keys))
	for i, key := range keys {
		rows[i] = driver.DerivedRow{Key: key, Value: []byte{byte(i), 0, 255}}
	}
	tx, header := beginDerivedPrefixTest(t, s, lease, rows)
	commitDerivedPrefixTest(t, tx, header)
	r, err := s.Read(ctx, driver.View{Incarnation: s.state.meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"raw/\xff/", "raw/\xfe/", "raw/\ufffd/"})
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	if err != nil || !reflect.DeepEqual(got, rows) {
		t.Fatalf("opaque rows=%+v err=%v", got, err)
	}
	tx, _ = beginDerivedPrefixTest(t, s, lease, rows)
	if err = tx.PutDerived(ctx, driver.DerivedAncestorClosure, rows[:1], nil); err != nil {
		t.Fatal(err)
	}
	got, err = tx.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"raw/"})
	if err != nil || !reflect.DeepEqual(got, rows[1:]) {
		t.Fatalf("staged opaque rows=%+v err=%v", got, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, []string{"raw/"})
	if err != nil || !reflect.DeepEqual(got, rows) {
		t.Fatalf("rollback changed prefix index: %v", err)
	}
}
