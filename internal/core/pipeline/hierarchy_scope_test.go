package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This structural capability is intentionally optional. The fallback control
// hides it while using the very same real SQLite snapshot and Writer.
type prefixRowsReader interface {
	DerivedRowsForPrefixes(context.Context, driver.DerivedTable, []string) ([]driver.DerivedRow, error)
}

type closureScopeStore struct {
	*sqlite.Store
	hide                   bool
	mu                     sync.Mutex
	rows, batches, singles int
	observed               map[string]driver.LockState
}

func (s *closureScopeStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return r, err
	}
	allocator, ok := r.(driver.AllocatorReader)
	if !ok {
		_ = r.Close()
		return nil, driver.ErrUnsupported
	}
	base := &closureScopeReader{Reader: r, AllocatorReader: allocator, store: s}
	if s.hide {
		return base, nil
	}
	return &closurePrefixReader{closureScopeReader: base}, nil
}

type closureScopeReader struct {
	driver.Reader
	driver.AllocatorReader
	store *closureScopeStore
}

func (r *closureScopeReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states, err := r.Reader.Observe(ctx, keys)
	if err == nil {
		r.store.mu.Lock()
		for _, state := range states {
			encoded, e := driver.EncodeLockKey(state.Key)
			if e != nil {
				r.store.mu.Unlock()
				return nil, e
			}
			r.store.observed[encoded] = state
		}
		r.store.mu.Unlock()
	}
	return states, err
}

func (r *closureScopeReader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	rows, err := r.Reader.DerivedRows(ctx, table, prefix)
	r.store.mu.Lock()
	r.store.singles++
	r.store.rows += len(rows)
	r.store.mu.Unlock()
	return rows, err
}

type closurePrefixReader struct{ *closureScopeReader }

func (r *closurePrefixReader) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	reader, ok := r.Reader.(prefixRowsReader)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	rows, err := reader.DerivedRowsForPrefixes(ctx, table, prefixes)
	r.store.mu.Lock()
	r.store.rows += len(rows)
	r.store.batches++
	r.store.mu.Unlock()
	return rows, err
}

func TestSQLiteRootClosureReadsStayWithinConsumedPrefixes(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := "batched"
		if fallback {
			name = "fallback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			value, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "closure-scope.db")})
			if err != nil {
				t.Fatal(err)
			}
			s := &closureScopeStore{Store: value.(*sqlite.Store), hide: fallback, observed: map[string]driver.LockState{}}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			lease, err := s.Lease(ctx, "closure-scope", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := lease.Release(ctx); err != nil {
					t.Error(err)
				}
			})
			next := 0
			w := New(s, lease, WithOntology(testOntology{}), WithIDGenerator(func() (string, error) {
				next++
				return fmt.Sprintf("pl-scope-%032x", next), nil
			}))
			ops := make([]model.Op, 100)
			for i := range ops {
				ops[i] = create("")
			}
			apply(t, w, model.ChangeSet{Ops: ops}, meta(1))
			s.mu.Lock()
			s.rows, s.batches, s.singles, s.observed = 0, 0, 0, map[string]driver.LockState{}
			s.mu.Unlock()
			commit := apply(t, w, model.ChangeSet{Ops: ops}, meta(2))
			if len(commit.Results) != 100 {
				t.Fatalf("results=%d", len(commit.Results))
			}
			// Every new root's source miss, exact closure sentinel and proposed
			// derived-row miss must still participate in optimistic validation.
			misses, sentinels, derived := 0, 0, 0
			s.mu.Lock()
			for _, state := range s.observed {
				switch state.Key.Kind {
				case driver.LockRecord:
					if !state.Present {
						misses++
					}
				case driver.LockSentinel:
					if strings.HasPrefix(string(state.Key.Sentinel), "closure/") && !state.Present {
						sentinels++
					}
				case driver.LockDerived:
					if state.Key.Derived.Table == driver.DerivedAncestorClosure && !state.Present {
						derived++
					}
				}
			}
			rows, batches, singles := s.rows, s.batches, s.singles
			s.mu.Unlock()
			if misses != 100 || sentinels != 100 || derived != 100 {
				t.Fatalf("guarded source/closure/derived misses=%d/%d/%d, want100/100/100", misses, sentinels, derived)
			}
			if !fallback && (rows != 0 || batches != 2 || singles != 0) {
				t.Fatalf("100 new roots enumerated %d unrelated closure rows in %d prefix batches/%d single calls; want0rows/2batches/0singles", rows, batches, singles)
			}
			if fallback && (rows != 100 || batches != 0 || singles != 101) {
				t.Fatalf("fallback control rows/batches/singles=%d/%d/%d, want100/0/101", rows, batches, singles)
			}
			m, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Store.Read(ctx, driver.View{Incarnation: m.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			all, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
			if err != nil || len(all) != 200 {
				t.Fatalf("durable closure census=%d err=%v", len(all), err)
			}
			for _, row := range all {
				descendant, ancestor, depth, err := DecodeAncestorRow(row)
				if err != nil || descendant != ancestor || depth != 0 {
					t.Fatalf("durable root closure=%+v err=%v", row, err)
				}
			}
			t.Logf("100-operation real SQLite root commit: %d enumerated rows, %d prefix batches, %d single calls, 100 guarded source/closure/derived misses; durable200selfrows; fallback=%t", rows, batches, singles, fallback)
		})
	}
}
