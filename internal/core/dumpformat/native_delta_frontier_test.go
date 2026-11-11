package dumpformat

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeDeltaPhysicalKeyOwnershipSurvivesValidationScratch(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	root := NewNativeDeltaBudget(0)
	var owners []*NativeDeltaBudget
	var retained uint64
	for _, id := range []string{"1", "2"} {
		row := deltaTestRow(t, table, id, "exact", 0)
		owner := root.Scope()
		owners = append(owners, owner)
		fields, key, err := validateNativeDeltaRowAccounted(table, row, nil, owner)
		if err != nil || fields["value"] != "exact" || len(key) == 0 {
			t.Fatal("valid row changed", fields, key, err)
		}
		n := uint64(base64.StdEncoding.DecodedLen(len(row.Key)))
		retained += n
		if owner.ReservedBytes("physical_key_decode") != n {
			t.Fatal("returned key lost its owner", owner.Usage())
		}
		owner.ReleaseBytes("normalized_fields", owner.ReservedBytes("normalized_fields"))
		if root.Usage().CurrentAccountedDecodedBytes != retained {
			t.Fatal("previous/current key overlap is not charged", root.Usage(), retained)
		}
	}
	for _, owner := range owners {
		owner.Release()
	}
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("key owners did not unwind", root.Usage())
	}
	// The string copy has a separate admission from the decoded byte buffer.
	key := []byte("physical-key")
	limited := NewNativeDeltaBudget(uint64(len(key) - 1))
	owner := limited.Scope()
	value, err := nativeDeltaMapKeyAccounted(key, owner)
	var refusal *NativeDeltaDecodedLimitError
	if value != "" || !errors.As(err, &refusal) || refusal.Phase != "decoded_row_map_keys" || refusal.MeasuredBytes != uint64(len(key)) || refusal.Overflow {
		t.Fatal("map key was not refused before conversion", value, err)
	}
	owner.Release()
	if limited.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("map key refusal retained a candidate")
	}
}

func TestNativeDeltaReconstructionDropsDecodedKeysAndMapReplacements(t *testing.T) {
	f := deltaReconstructionFixture(t)
	root := NewNativeDeltaBudget(0)
	var simultaneousKeys uint64
	root.Observe(func(event NativeDeltaAllocationEvent) {
		if n := root.ReservedBytes("physical_key_decode"); n > simultaneousKeys {
			simultaneousKeys = n
		}
	})
	_, _, err := reconstructNativeDelta(context.Background(), &f.stream, f.root, root)
	if err != nil {
		t.Fatal(err)
	}
	usage := root.Usage()
	if simultaneousKeys == 0 || usage.CumulativeCharges["decoded_row_map_keys"] == 0 || usage.CurrentCategories["physical_key_decode"] != 0 || usage.CurrentCategories["decoded_row_map_keys"] != 0 {
		t.Fatal("reconstruction key owners did not end with their tables", simultaneousKeys, usage)
	}
}

func TestNativeDeltaGraphFrontierReleasesOnlyLastUsedUnsharedFiles(t *testing.T) {
	directory := t.TempDir()
	path := func(name string) string {
		p := filepath.Join(directory, name)
		if err := os.WriteFile(p, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	shared, old, previous := path("shared"), path("old"), path("previous")
	root := NewNativeDeltaBudget(100)
	owner := root.Scope()
	defer owner.Release()
	for i := 0; i < 2; i++ {
		if err := owner.ChargeBytes("staged_state_metadata", 10); err != nil {
			t.Fatal(err)
		}
	}
	r := nativeDeltaReconstructor{ctx: context.Background(), dir: directory, budget: owner,
		lastUse: map[string]uint64{"expired": 2, "future": 5}, stagedBytes: map[string]uint64{"expired": 10, "future": 10},
		states: map[string]deltaStagedState{
			"expired": {tables: map[string]deltaStagedTable{"shared": {path: shared}, "old": {path: old}}},
			"future":  {tables: map[string]deltaStagedTable{"shared": {path: shared}}},
		}, previousTables: map[string]deltaStagedTable{"previous": {path: previous}}}
	if err := r.releaseGraphFrontier(2); err != nil {
		t.Fatal(err)
	}
	if len(r.states) != 1 || owner.ReservedBytes("staged_state_metadata") != 10 {
		t.Fatal("state metadata survived last use")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("superseded file retained", err)
	}
	for _, p := range []string{shared, previous} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("live shared/previous file dropped", err)
		}
	}
	r.previousTables = nil
	if err := r.releaseGraphFrontier(5); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 || root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("terminal frontier retained files/reservations", files, err, root.Usage())
	}
}

func TestNativeRowBudgetedDecodeMatchesPureCodecAndRollsBackOnRefusal(t *testing.T) {
	table := NativeTable{Columns: []string{"id", "value"}, Rows: []Row{{"id": "x", "value": "exact<>&\n"}}}
	records, err := NativeV1TableRecords(rowTestSource, "items", nativeRetainedHistory, table)
	if err != nil {
		t.Fatal(err)
	}
	root := NewNativeDeltaBudget(0)
	for _, record := range records {
		owner := root.Scope()
		archive := &Archive{nativeBudget: owner}
		meta, row, err := archive.DecodeNativeRowRecord(record)
		expectedMeta, expectedRow, pureErr := DecodeNativeRowRecord(record)
		if err != nil || pureErr != nil || !equalJSON(meta, expectedMeta) || !equalJSON(row, expectedRow) {
			t.Fatal("budget changed codec", err, pureErr)
		}
		owner.Release()
	}
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("decode ownership leaked", root.Usage())
	}
	for _, limit := range []uint64{1, 64} {
		root := NewNativeDeltaBudget(limit)
		owner := root.Scope()
		_, _, err := (&Archive{nativeBudget: owner}).DecodeNativeRowRecord(records[0])
		if err == nil {
			t.Fatal("unadmitted decode accepted")
		}
		owner.Release()
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("decode failure leaked")
		}
	}
}
