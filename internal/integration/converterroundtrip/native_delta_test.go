package converterroundtrip

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeDeltaBatchAccountingMatchesActualLoweredBytes(t *testing.T) {
	identity := dumpformat.SourceIdentity{StoreID: "public-store", Project: "public"}
	table := func(value string) dumpformat.NativeTable {
		return dumpformat.NativeTable{Columns: []string{"value"}, Rows: []dumpformat.Row{{"value": value}}}
	}
	states := []map[string]dumpformat.NativeTable{
		{"one": table("<first>&"), "dropped": table("old")},
		{"one": table("changed"), "added": table("new")},
		{"one": table("changed"), "added": table("new")},
		{},
	}
	before := []driver.Record{}
	var previous map[string]dumpformat.NativeTable
	for i, tables := range states {
		image := dumpformat.NativeHistoryImage{Sequence: uint64(i + 1), CommitID: "public-commit", Tables: tables}
		got, err := nativeDeltaBatchSize(uint64(i+1), identity, image, before, previous)
		if err != nil {
			t.Fatal(err)
		}
		after, err := nativeImageRecords(identity, tables)
		if err != nil {
			t.Fatal(err)
		}
		batch := driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: uint64(i + 1), CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: historyDelta(before, after)}
		raw, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		if got != uint64(len(raw)+1) {
			t.Fatalf("transition %d measured=%d actual=%d", i, got, len(raw)+1)
		}
		before = after
		previous = tables
	}
}
