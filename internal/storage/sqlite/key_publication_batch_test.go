package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Forward every call to the real engine. RowsAffected counts inserted rows,
// not trigger work, executor row visits, or a completed Commit's total work.
type keyPublicationWork struct {
	selectionCloseWork
	maxArgs int
}

func (w *keyPublicationWork) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if len(args) > w.maxArgs {
		w.maxArgs = len(args)
	}
	return w.selectionCloseWork.ExecContext(ctx, query, args...)
}

func keyPublicationInputs(t *testing.T, count int) (map[string]driver.LockState, map[string]driver.LockKey) {
	t.Helper()
	derived := make(map[string]driver.LockState, count)
	bumps := make(map[string]driver.LockKey, count)
	for i := 0; i < count; i++ {
		row := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: fmt.Sprintf("key-batch-%04d", i)}
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &row}
		state := driver.LockState{Key: key, Revision: model.Natural64(9)}
		if i%3 != 0 {
			value := []byte(fmt.Sprintf("value %d: \" <>& 雪", i))
			state.Present = true
			state.Derived = &driver.DerivedRow{Key: row.Key, Value: value}
			if i%3 == 1 {
				source := testKey(fmt.Sprintf("key-batch-source-%04d", i))
				state.Derived.Index = &driver.SelectionIndex{
					Version: 1, Row: row, Incarnation: "key-batch-incarnation",
					Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
					Source: driver.SelectionSource{Key: source, Revision: model.Natural64(8), Present: true, Digest: "source-digest"},
					Bead:   &driver.SelectionBead{Ref: *source.Ref, Type: "task", Title: "雪 <>&", MembershipLeaf: "leaf"},
				}
			}
		}
		derived[keyID(key)] = state
		var bump driver.LockKey
		if i%2 == 0 {
			sentinel, err := driver.NewReadySentinel("key-batch", fmt.Sprintf("task-%04d", i))
			if err != nil {
				t.Fatal(err)
			}
			bump = driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		} else {
			bump = driver.LockKey{Kind: driver.LockReadFact, Fact: driver.ReadFact(fmt.Sprintf("key-batch/fact-%04d", i))}
		}
		bumps[keyID(bump)] = bump
	}
	return derived, bumps
}

type keyPublicationImage struct {
	key, revision string
	data          []byte
}

func keyPublicationImages(t *testing.T, s *Store) []keyPublicationImage {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT key,rev,data FROM key_history ORDER BY key,rev`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var images []keyPublicationImage
	for rows.Next() {
		var image keyPublicationImage
		if err := rows.Scan(&image.key, &image.revision, &image.data); err != nil {
			t.Fatal(err)
		}
		images = append(images, image)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return images
}

func keyPublicationJournal(t *testing.T, s *Store) map[string]string {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT domain,key FROM ready_order_changes`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	journal := make(map[string]string)
	for rows.Next() {
		var domain, key string
		if err := rows.Scan(&domain, &key); err != nil {
			t.Fatal(err)
		}
		journal[key] = domain
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return journal
}

// This raw marker tests invalidation only. It supplies no certified generation,
// digest or other fields required to advertise a Ready capability.
func keyPublicationTriggerMarker(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), `UPDATE ready_order_fence SET dirty=0,generation='key-batch-before' WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
}

func checkKeyPublicationFence(t *testing.T, s *Store, changed bool) {
	t.Helper()
	var dirty, ever int
	var generation string
	if err := s.db.QueryRowContext(context.Background(), `SELECT dirty,ever_source,generation FROM ready_order_fence WHERE singleton=1`).Scan(&dirty, &ever, &generation); err != nil {
		t.Fatal(err)
	}
	want := 0
	if changed {
		want = 1
	}
	if dirty != want || ever != want || (generation != "key-batch-before") != changed {
		t.Fatalf("raw key trigger dirty=%d ever_source=%d generation=%q changed=%t", dirty, ever, generation, changed)
	}
}

func TestKeyPublicationBatchesMatchScalarImagesAndTriggers(t *testing.T) {
	for _, count := range []int{0, 127, 128, 129, 1000, 2000} {
		t.Run(fmt.Sprintf("keys_%d", count), func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t, ctx, t.TempDir()+"/batched.db")
			reference := openStore(t, ctx, t.TempDir()+"/scalar.db")
			keyPublicationTriggerMarker(t, s)
			keyPublicationTriggerMarker(t, reference)
			derived, bumps := keyPublicationInputs(t, count)
			before, err := json.Marshal(derived)
			if err != nil {
				t.Fatal(err)
			}
			revision := model.Natural64(^uint64(0))
			var derivedWork, bumpWork keyPublicationWork
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				derivedWork = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
				if err := publishDerivedKeys(ctx, &derivedWork, revision, derived); err != nil {
					return err
				}
				bumpWork = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
				return publishBumpKeys(ctx, &bumpWork, revision, bumps)
			}); err != nil {
				t.Fatal(err)
			}
			if err := reference.immediate(ctx, nil, func(c *sql.Conn) error {
				for _, state := range derived {
					state.Revision = revision
					if err := publishKey(ctx, c, state); err != nil {
						return err
					}
				}
				for _, key := range bumps {
					if err := publishKey(ctx, c, driver.LockState{Key: key, Revision: revision, Present: true}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(derived)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("publication mutated staged states: %v", err)
			}
			images, scalar := keyPublicationImages(t, s), keyPublicationImages(t, reference)
			if len(images) != 2*count || !reflect.DeepEqual(images, scalar) {
				t.Fatalf("batched rows=%d scalar rows=%d; exact key/revision/JSON images differ", len(images), len(scalar))
			}
			wantJournal := make(map[string]string, 2*count)
			for key := range derived {
				wantJournal[key] = "state"
			}
			for key := range bumps {
				wantJournal[key] = "state"
			}
			if !reflect.DeepEqual(keyPublicationJournal(t, s), wantJournal) || !reflect.DeepEqual(keyPublicationJournal(t, reference), wantJournal) {
				t.Fatal("batched or scalar row triggers lost an exact state key")
			}
			checkKeyPublicationFence(t, s, count != 0)
			checkKeyPublicationFence(t, reference, count != 0)
			maxArgs := 3 * min(count, keyPublicationBatchSize)
			for name, work := range map[string]keyPublicationWork{"derived": derivedWork, "bumps": bumpWork} {
				if work.statements != (count+127)/128 || work.rows != int64(count) || work.maxArgs != maxArgs {
					t.Fatalf("%s actual_exec=%d rows_affected=%d max_bindings=%d for %d keys", name, work.statements, work.rows, work.maxArgs, count)
				}
				t.Logf("%s keys=%d actual_exec=%d rows_affected=%d max_bindings=%d executor_row_visits=UNMEASURED", name, count, work.statements, work.rows, work.maxArgs)
			}
		})
	}
}

func TestKeyPublicationBatchesRollbackAndCancellation(t *testing.T) {
	for _, kind := range []string{"derived", "bumps"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t, ctx, t.TempDir()+"/rollback.db")
			keyPublicationTriggerMarker(t, s)
			const count = 2*keyPublicationBatchSize + 1
			derived, bumps := keyPublicationInputs(t, count)
			publish := func(ctx context.Context, c selectionPublicationExecutor) error {
				if kind == "derived" {
					return publishDerivedKeys(ctx, c, model.Natural64(10), derived)
				}
				return publishBumpKeys(ctx, c, model.Natural64(10), bumps)
			}
			checkEmpty := func() {
				t.Helper()
				if len(keyPublicationImages(t, s)) != 0 || len(keyPublicationJournal(t, s)) != 0 {
					t.Fatal("aborted batch left key images or Ready journal entries")
				}
				checkKeyPublicationFence(t, s, false)
			}
			abort := errors.New("abort after three actual INSERT batches")
			var work keyPublicationWork
			err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
				if err := publish(ctx, &work); err != nil {
					return err
				}
				return abort
			})
			if !errors.Is(err, abort) || work.statements != 3 || work.rows != count {
				t.Fatalf("late abort actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
			}
			checkEmpty()
			// Cancel after the first successful engine INSERT; the transaction
			// must roll it back when the next batch observes cancellation.
			cancelled, cancel := context.WithCancel(ctx)
			defer cancel()
			err = s.immediate(cancelled, nil, func(c *sql.Conn) error {
				work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c, cancel: cancel}}
				return publish(cancelled, &work)
			})
			if !errors.Is(err, context.Canceled) || work.statements != 1 || work.rows != keyPublicationBatchSize {
				t.Fatalf("after-execution cancellation actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
			}
			checkEmpty()
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
				return publish(ctx, &work)
			}); err != nil {
				t.Fatal(err)
			}
			if len(keyPublicationImages(t, s)) != count || len(keyPublicationJournal(t, s)) != count || work.statements != 3 || work.rows != count {
				t.Fatal("successful recovery lost rows, journal entries or actual batch executions")
			}
			checkKeyPublicationFence(t, s, true)
		})
	}
}

func TestKeyPublicationBatchesDuplicateRefusesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, t.TempDir()+"/duplicate.db")
	keyPublicationTriggerMarker(t, s)
	var work keyPublicationWork
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
		var batch keyPublicationBatch
		for i := 0; i < 2*keyPublicationBatchSize; i++ {
			state := driver.LockState{Key: testKey(fmt.Sprintf("duplicate-%04d", i)), Revision: model.Natural64(10), Present: true}
			if err := batch.add(ctx, &work, state); err != nil {
				return err
			}
		}
		// A plain INSERT must refuse the duplicate, never replace or ignore it.
		if err := batch.add(ctx, &work, driver.LockState{Key: testKey("duplicate-0000"), Revision: model.Natural64(10)}); err != nil {
			return err
		}
		return batch.flush(ctx, &work)
	})
	if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed") || work.statements != 2 || work.rows != 2*keyPublicationBatchSize {
		t.Fatalf("duplicate actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	if len(keyPublicationImages(t, s)) != 0 || len(keyPublicationJournal(t, s)) != 0 {
		t.Fatal("duplicate refusal failed to roll back earlier batches and row triggers")
	}
	checkKeyPublicationFence(t, s, false)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return publishKey(ctx, c, driver.LockState{Key: testKey("duplicate-0000"), Revision: model.Natural64(10), Present: true})
	}); err != nil {
		t.Fatalf("scalar recovery after duplicate refusal: %v", err)
	}
	if len(keyPublicationImages(t, s)) != 1 || len(keyPublicationJournal(t, s)) != 1 {
		t.Fatal("duplicate refusal left writer unusable")
	}
	checkKeyPublicationFence(t, s, true)
}
