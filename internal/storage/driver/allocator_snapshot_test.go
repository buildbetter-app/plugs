package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func allocatorSnapshotFixture(t *testing.T) AllocatorSnapshot {
	t.Helper()
	s, err := NewAllocatorSnapshot(Meta{StoreID: "source", Incarnation: "original"}, model.Natural64(7), model.Natural64(0), AllocatorInventory{
		Projects: []OfflineAllocator{{Prefix: "seq", Mode: AllocatorSequential, LastID: ^uint64(0), Children: map[string]uint64{"seq-1": ^uint64(0)}}},
		Parents:  []ParentCounter{{Incarnation: "original", Parent: "seq-1", LastChild: ^uint64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAllocatorSnapshotCanonicalLossless(t *testing.T) {
	s := allocatorSnapshotFixture(t)
	raw, err := EncodeAllocatorSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(raw, []byte("18446744073709551615")) != 3 {
		t.Fatalf("terminal counters lost: %s", raw)
	}
	decoded, err := DecodeAllocatorSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := s.Binding()
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Match(decoded); err != nil {
		t.Fatal(err)
	}
	detached, err := s.Detached()
	if err != nil {
		t.Fatal(err)
	}
	detached.Inventory.Projects[0].Children["seq-1"] = 0
	if s.Inventory.Projects[0].Children["seq-1"] != ^uint64(0) {
		t.Fatal("detachment aliases caller")
	}
	for _, change := range []func(*AllocatorSnapshot){
		func(v *AllocatorSnapshot) { v.StoreID = "other" },
		func(v *AllocatorSnapshot) { v.At = model.Natural64(8) },
		func(v *AllocatorSnapshot) { v.Inventory.Parents[0].LastChild-- },
	} {
		copy, _ := s.Detached()
		change(&copy)
		if binding.Match(copy) == nil {
			t.Fatal("changed snapshot accepted")
		}
	}
}

func TestAllocatorSnapshotStrictPayloadRefusals(t *testing.T) {
	s := allocatorSnapshotFixture(t)
	raw, _ := EncodeAllocatorSnapshot(s)
	for name, bad := range map[string][]byte{
		"whitespace":        append(bytes.Clone(raw), ' '),
		"duplicate-root":    bytes.Replace(raw, []byte(`"version":`), []byte(`"version":"x","version":`), 1),
		"duplicate-child":   bytes.Replace(raw, []byte(`"seq-1":18446744073709551615`), []byte(`"seq-1":0,"seq-1":18446744073709551615`), 1),
		"unknown-field":     bytes.Replace(raw, []byte(`"at":7`), []byte(`"extra":true,"at":7`), 1),
		"missing-parents":   bytes.Replace(raw, []byte(`"parents":[`), []byte(`"absent":[`), 1),
		"wrong-incarnation": bytes.Replace(raw, []byte(`"incarnation":"original","parent"`), []byte(`"incarnation":"old","parent"`), 1),
		"float-counter":     bytes.Replace(raw, []byte(`18446744073709551615`), []byte(`18446744073709551615.0`), 1),
		"null-projects":     []byte(`{"version":"plug-native-allocator/1","store_id":"source","incarnation":"original","at":0,"coverage_floor":0,"projects":null,"parents":[]}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeAllocatorSnapshot(bad); err == nil {
				t.Fatal("invalid payload accepted")
			}
		})
	}
	empty, err := NewAllocatorSnapshot(Meta{StoreID: "source", Incarnation: "original"}, model.Natural64(0), model.Natural64(0), AllocatorInventory{})
	if err != nil {
		t.Fatal(err)
	}
	emptyRaw, _ := EncodeAllocatorSnapshot(empty)
	if !strings.Contains(string(emptyRaw), `"at":0,"coverage_floor":0,"projects":[],"parents":[]`) {
		t.Fatal(string(emptyRaw))
	}
	if _, err := DecodeAllocatorSnapshot(emptyRaw); err != nil {
		t.Fatal(err)
	}
}

func TestAllocatorJournalExactHeadCoverageAndCorruption(t *testing.T) {
	meta := Meta{StoreID: "source", Incarnation: "original", Head: model.Natural64(0)}
	zero, _ := NewAllocatorSnapshot(meta, meta.Head, meta.Head, AllocatorInventory{})
	rows, err := AllocatorJournalEntries(zero)
	if err != nil {
		t.Fatal(err)
	}
	one := allocatorSnapshotFixture(t)
	one.At = model.Natural64(1)
	entries, err := AllocatorJournalDelta(zero, one)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range entries {
		rows[k] = v
	}
	a := one.Inventory.Projects[0]
	rows[a.Prefix], _ = json.Marshal(a)
	p := one.Inventory.Parents[0]
	key, _ := ParentCounterKey(p.Incarnation, p.Parent)
	rows[key], _ = json.Marshal(p)
	meta.Head = one.At
	for _, at := range []uint64{0, 1} {
		got, err := MaterializeAllocatorJournal(meta, model.Natural64(at), rows)
		if err != nil {
			t.Fatal(err)
		}
		if at == 0 && len(got.Inventory.Projects) != 0 {
			t.Fatal("At0 substituted latest")
		}
		if at == 1 && got.Inventory.Parents[0].LastChild != ^uint64(0) {
			t.Fatal("counter lost")
		}
	}
	two, _ := one.Detached()
	two.At = model.Natural64(2)
	delta, err := AllocatorJournalDelta(one, two)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) != 1 {
		t.Fatalf("unchanged values copied: %d entries", len(delta))
	}
	for _, mutate := range []func(map[string][]byte){
		func(m map[string][]byte) { m[AllocatorJournalNamespace+"unknown"] = []byte(`{}`) },
		func(m map[string][]byte) { delete(m, key) },
		func(m map[string][]byte) {
			for k := range m {
				if strings.Contains(k, "/value/") {
					delete(m, k)
					break
				}
			}
		},
		func(m map[string][]byte) { delete(m, AllocatorJournalIndexKey(meta.Incarnation, model.Natural64(0))) },
	} {
		copy := map[string][]byte{}
		for k, v := range rows {
			copy[k] = bytes.Clone(v)
		}
		mutate(copy)
		if _, err := MaterializeAllocatorJournal(meta, one.At, copy); err == nil {
			t.Fatal("corrupt/incomplete history accepted")
		}
	}
	legacy := map[string][]byte{a.Prefix: rows[a.Prefix], key: rows[key]}
	if _, err := MaterializeAllocatorJournal(meta, model.Natural64(0), legacy); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("legacy old-head evidence: %v", err)
	}
	current, err := MaterializeAllocatorJournal(meta, meta.Head, legacy)
	if err != nil || current.CoverageFloor.String() != "1" {
		t.Fatalf("legacy coverage: %+v %v", current, err)
	}
}
