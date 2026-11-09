package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Retain the original W5 family admission/refusal and real Commit controls.
func TestSelectionPredicateFamilyIndexWorkAndCommit(t *testing.T) {
	ready, err := driver.NewReadySentinel("project", "task")
	if err != nil {
		t.Fatal(err)
	}
	closure, err := driver.NewClosureSentinel("root")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := driver.NewGrantSentinel("scope")
	if err != nil {
		t.Fatal(err)
	}
	makeDerived := func(table driver.DerivedTable, name string) driver.LockState {
		key := driver.DerivedRowKey{Table: table, Key: name}
		return driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &key}}
	}
	staged := &transaction{derived: make(map[string]driver.LockState), bumps: make(map[string]driver.LockKey)}
	for i := 0; i < 257; i++ {
		state := makeDerived(driver.DerivedReadyState, fmt.Sprintf("ready-%03d", i))
		staged.derived[keyID(state.Key)] = state
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedSortKey,
	} {
		state := makeDerived(table, "other")
		staged.derived[keyID(state.Key)] = state
	}
	for i := 0; i < 129; i++ {
		sentinel, err := driver.NewReadySentinel("project", fmt.Sprintf("task-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		staged.bumps[keyID(key)] = key
	}
	for _, sentinel := range []driver.Sentinel{closure, grant, driver.SentinelOntology, driver.SentinelPlugins} {
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		staged.bumps[keyID(key)] = key
	}
	readFact := driver.LockKey{Kind: driver.LockReadFact, Fact: driver.ReadFact(ready)}
	staged.bumps[keyID(readFact)] = readFact
	work, err := staged.validateDerivedPredicates()
	if err != nil {
		t.Fatal(err)
	}
	if work.BumpsVisited != len(staged.bumps) || work.DerivedVisited != len(staged.derived) ||
		work.FamilyChecks != len(staged.derived)-1 {
		t.Fatalf("predicate work %+v for %d bumps and %d staged rows", work, len(staged.bumps), len(staged.derived))
	}
	grantKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: grant}
	delete(staged.bumps, keyID(grantKey))
	if _, err := staged.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "derived table grant requires grant/ predicate bump") {
		t.Fatalf("missing grant family accepted: %v", err)
	}
	wrongKind := &transaction{
		derived: map[string]driver.LockState{"ready": makeDerived(driver.DerivedReadyState, "ready")},
		bumps:   map[string]driver.LockKey{keyID(readFact): readFact},
	}
	if _, err := wrongKind.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "requires ready/ predicate bump") {
		t.Fatalf("read-fact text counted as a ready sentinel: %v", err)
	}
	wrongKind.bumps = map[string]driver.LockKey{"invalid-ontology": {
		Kind: driver.LockSentinel, Sentinel: "ontology/extra"}}
	wrongKind.derived = map[string]driver.LockState{"ontology": makeDerived(driver.DerivedOntology, "ontology")}
	if _, err := wrongKind.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "requires ontology predicate bump") {
		t.Fatalf("ontology prefix counted as exact sentinel: %v", err)
	}

	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	var extra []driver.LockKey
	var rows []driver.DerivedRow
	var sentinels []driver.Sentinel
	for i := 0; i < 257; i++ {
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("committed-%03d", i)}
		extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
		rows = append(rows, driver.DerivedRow{Key: row.Key, Value: []byte(`{"ready":true}`)})
	}
	for i := 0; i < 129; i++ {
		sentinel, err := driver.NewReadySentinel("committed", fmt.Sprintf("task-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		sentinels = append(sentinels, sentinel)
	}
	positive, header := beginSourceTestWithLocks(t, s, lease, extra, true)
	if err := positive.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err := positive.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	committedWork, err := positive.validateDerivedPredicates()
	if err != nil || committedWork.BumpsVisited != len(positive.bumps) ||
		committedWork.DerivedVisited != len(rows) || committedWork.FamilyChecks != len(rows) {
		t.Fatalf("real staged predicate work %+v err=%v", committedWork, err)
	}
	entry, err := positive.Commit(ctx, header)
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("ready-family Commit: %+v %v", entry, err)
	}
	commitOne := func(name string, family driver.Sentinel) (model.Changeset, error) {
		t.Helper()
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: name}
		rowKey := driver.LockKey{Kind: driver.LockDerived, Derived: &row}
		familyKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: family}
		tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{rowKey, familyKey}, true)
		if err := tx.PutDerived(ctx, row.Table, nil, []driver.DerivedRow{{Key: row.Key, Value: []byte(`{"ready":true}`)}}); err != nil {
			t.Fatal(err)
		}
		if err := tx.BumpSentinels(ctx, []driver.Sentinel{family}); err != nil {
			t.Fatal(err)
		}
		return tx.Commit(ctx, header)
	}
	_, err = commitOne("refused", grant)
	if err == nil || !strings.Contains(err.Error(), "derived table ready_state requires ready/ predicate bump") {
		t.Fatalf("wrong-family Commit: %v", err)
	}
	meta, err := s.Meta(ctx)
	if err != nil || meta.Head.String() != "1" {
		t.Fatalf("refused Commit changed Head: %+v %v", meta, err)
	}
}

func TestSelectionPredicateFamilyWorkAtCommitPopulations(t *testing.T) {
	for _, population := range []int{1000, 2000} {
		t.Run(fmt.Sprintf("derived_%d", population), func(t *testing.T) {
			ctx := testContext(t)
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			ready, err := driver.NewReadySentinel("*", "*")
			if err != nil {
				t.Fatal(err)
			}
			sentinels := []driver.Sentinel{ready}
			extra := []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: ready}}
			rows := make([]driver.DerivedRow, population)
			for i := range rows {
				key := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("predicate-work-%04d", i)}
				extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
				rows[i] = driver.DerivedRow{Key: key.Key, Value: []byte(`{"ready":true}`)}
				closure, err := driver.NewClosureSentinel(fmt.Sprintf("predicate-source-%04d", i))
				if err != nil {
					t.Fatal(err)
				}
				extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
				sentinels = append(sentinels, closure)
			}
			tx, header := beginSourceTestWithLocks(t, s, lease, extra, true)
			if err := tx.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
				t.Fatal(err)
			}
			if err := tx.BumpSentinels(ctx, sentinels); err != nil {
				t.Fatal(err)
			}
			// These visits occur inside the actual validator used by Commit,
			// over a real staged transaction with unrelated closure bumps.
			work, err := tx.validateDerivedPredicates()
			if err != nil || work.BumpsVisited != population+2 || work.DerivedVisited != population || work.FamilyChecks != population {
				t.Fatalf("population=%d actual predicate validation work=%+v: %v", population, work, err)
			}
			expectedDurable := len(tx.derived) + len(tx.bumps)
			started := time.Now()
			entry, err := tx.Commit(ctx, header)
			elapsed := time.Since(started)
			if err != nil || entry.Revision.String() != "1" {
				t.Fatalf("population=%d real Commit: %+v %v", population, entry, err)
			}
			var durable int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM key_history`).Scan(&durable); err != nil || durable != expectedDurable {
				t.Fatalf("population=%d durable keys=%d want=%d: %v", population, durable, expectedDurable, err)
			}
			t.Logf("population=%d validator_bumps_visited=%d validator_derived_visited=%d validator_family_checks=%d real_commit_elapsed_ns=%d durable_keys=%d", population, work.BumpsVisited, work.DerivedVisited, work.FamilyChecks, elapsed.Nanoseconds(), durable)
		})
	}
}
