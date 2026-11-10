package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func predicateRow(table driver.DerivedTable, key string) driver.LockState {
	return driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: key}}}
}

func TestPredicateBumpsRemainFamilyScoped(t *testing.T) {
	for _, test := range []struct {
		name      string
		table     driver.DerivedTable
		family    string
		sentinel  driver.Sentinel
		unrelated driver.Sentinel
	}{
		{"ready", driver.DerivedReadyState, "ready/", "ready/project/issue", "ready-other/project/issue"},
		{"ancestor", driver.DerivedAncestorClosure, "closure/", "closure/source", "closure-other/source"},
		{"cut", driver.DerivedCutClosure, "closure/", "closure/source", "ready/project/issue"},
		{"attribute", driver.DerivedAttribute, "closure/", "closure/source", "grant/principal"},
		{"grant", driver.DerivedGrant, "grant/", "grant/principal", "grants/principal"},
		{"ontology", driver.DerivedOntology, "ontology", "ontology", "ontology/child"},
		{"plugins", driver.DerivedPlugins, "plugins", "plugins", "plugins/child"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &transaction{
				derived: map[string]driver.LockState{"row": predicateRow(test.table, "row")},
				bumps:   map[string]driver.LockKey{"bump": {Kind: driver.LockSentinel, Sentinel: test.sentinel}},
			}
			if err := tx.validatePredicates(context.Background()); err != nil {
				t.Fatalf("complete family proof refused: %v", err)
			}
			for _, key := range []driver.LockKey{
				{Kind: driver.LockSentinel, Sentinel: test.unrelated},
				{Kind: driver.LockRecord, Sentinel: test.sentinel},
			} {
				tx.bumps["bump"] = key
				err := tx.validatePredicates(context.Background())
				if err == nil || !strings.Contains(err.Error(), "requires "+test.family+" predicate bump") {
					t.Fatalf("unrelated bump authorized %s rows: %v", test.family, err)
				}
			}
		})
	}
}

func TestPredicateBumpProofIsInvocationLocal(t *testing.T) {
	tx := &transaction{
		derived: make(map[string]driver.LockState, 5000),
		bumps: map[string]driver.LockKey{
			"ready":   {Kind: driver.LockSentinel, Sentinel: "ready/project/issue"},
			"closure": {Kind: driver.LockSentinel, Sentinel: "closure/source"},
			"grant":   {Kind: driver.LockSentinel, Sentinel: "grant/principal"},
			"schema":  {Kind: driver.LockSentinel, Sentinel: driver.SentinelOntology},
			"plugins": {Kind: driver.LockSentinel, Sentinel: driver.SentinelPlugins},
		},
	}
	tables := []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins}
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("row-%04d", i)
		tx.derived[key] = predicateRow(tables[i%len(tables)], key)
	}
	if err := tx.validatePredicates(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := tx.bumps["ready"]
	delete(tx.bumps, "ready")
	if err := tx.validatePredicates(context.Background()); err == nil || !strings.Contains(err.Error(), "requires ready/ predicate bump") {
		t.Fatalf("prior call authorized a missing current bump: %v", err)
	}
	tx.bumps["ready"] = ready
	if err := tx.validatePredicates(context.Background()); err != nil {
		t.Fatalf("restored complete proof refused: %v", err)
	}
}

func TestPredicateBumpCensusPreservesEarlierRefusals(t *testing.T) {
	ref := model.RefByID("predicate-source", model.Natural64(0))
	tx := &transaction{
		acquired: []driver.LockRequest{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}},
		records:  map[string]*driver.Record{"source": {Ref: ref, Kind: "bead"}},
		derived:  map[string]driver.LockState{"row": predicateRow(driver.DerivedReadyState, "row")},
	}
	if err := tx.validatePredicates(context.Background()); err == nil || err.Error() != "postgres: update sentinel requires atomic bump" {
		t.Fatalf("derived family changed acquired-sentinel refusal: %v", err)
	}
	tx.acquired = nil
	if err := tx.validatePredicates(context.Background()); err == nil || err.Error() != "postgres: bead write requires exact bead sentinel bump" {
		t.Fatalf("derived family changed record-sentinel refusal: %v", err)
	}
}
