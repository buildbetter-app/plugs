package driver

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestCanonicalLockOrder(t *testing.T) {
	ref := model.RefByID("bead-a", model.Natural("1.0"))
	keys := []LockKey{
		{Kind: LockRecord, Ref: &ref},
		{Kind: LockReadFact, Fact: "predicate"},
		GraphBlockingSentinelLock,
		{Kind: LockDerived, Derived: &DerivedRowKey{Table: DerivedReadyState, Key: "bead-a"}},
	}
	want := []string{"0:id:bead-a:1", "1:predicate", "2:graph/blocking", "3:ready_state:bead-a"}
	requests := make(LockSet, len(keys))
	for i, key := range keys {
		encoded, err := EncodeLockKey(key)
		if err != nil || encoded != want[i] {
			t.Fatalf("key %d = %q, %v; want %q", i, encoded, err, want[i])
		}
		requests[i] = LockRequest{Key: key, Mode: LockUpdate}
	}
	if err := ValidateLockSet(requests); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHeaderLocks(keys, requests); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLockSet(LockSet{requests[1], requests[0]}); err == nil {
		t.Fatal("unsorted locks accepted")
	}
	if err := ValidateLockSet(LockSet{requests[0], requests[0]}); err == nil {
		t.Fatal("duplicate locks accepted")
	}
	if err := ValidateHeaderLocks(keys[:3], requests); err == nil {
		t.Fatal("incomplete commit declaration accepted")
	}
}

func TestSentinelCensus(t *testing.T) {
	for _, raw := range []string{"bead/b-1", "graph/blocking", "ready/project/task", "closure/b-1", "grant/project", "ontology", "plugins"} {
		if _, err := NewSentinel(raw); err != nil {
			t.Errorf("canonical sentinel %q: %v", raw, err)
		}
	}
	for _, raw := range []string{"", "ready-set", "closure", "grant", "plugin", "bead/", "bead/a/b", "ready/a", "ready/a/b/c", "grant/ x", "ontology ", "plugins\x00"} {
		if _, err := NewSentinel(raw); err == nil {
			t.Errorf("invalid sentinel %q accepted", raw)
		}
	}
}

func TestParameterizedSentinelConstructors(t *testing.T) {
	tests := []struct {
		name string
		make func(string) (Sentinel, error)
		want Sentinel
	}{
		{"bead", NewBeadSentinel, "bead/b-1"},
		{"closure", NewClosureSentinel, "closure/b-1"},
		{"grant", NewGrantSentinel, "grant/b-1"},
		{"ready project", func(s string) (Sentinel, error) { return NewReadySentinel(s, "task") }, "ready/b-1/task"},
		{"ready type", func(s string) (Sentinel, error) { return NewReadySentinel("project", s) }, "ready/project/b-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.make("b-1")
			if err != nil || got != tt.want {
				t.Fatalf("constructor = %q, %v; want %q", got, err, tt.want)
			}
			for _, part := range []string{"", " ", " a", "a ", "\ta", "a\n", "\u00a0a", "a/b", "a\x00b"} {
				if got, err := tt.make(part); err == nil || got != "" {
					t.Errorf("invalid component %q = %q, %v", part, got, err)
				}
			}
		})
	}
}

func TestLockKeysRejectMissingAndMixedForms(t *testing.T) {
	ref := model.RefByKey("project-1/task-1")
	derived := DerivedRowKey{Table: DerivedGrant, Key: "scope/subject"}
	keys := []LockKey{
		{},
		{Kind: "future"},
		{Kind: LockRecord},
		{Kind: LockRecord, Ref: &Ref{}},
		{Kind: LockReadFact},
		{Kind: LockSentinel},
		{Kind: LockDerived},
		{Kind: LockRecord, Ref: &ref, Fact: "fact"},
		{Kind: LockRecord, Ref: &ref, Sentinel: SentinelOntology},
		{Kind: LockRecord, Ref: &ref, Derived: &derived},
		{Kind: LockReadFact, Fact: "fact", Ref: &ref},
		{Kind: LockReadFact, Fact: "fact", Sentinel: SentinelOntology},
		{Kind: LockReadFact, Fact: "fact", Derived: &derived},
		{Kind: LockSentinel, Sentinel: SentinelOntology, Ref: &ref},
		{Kind: LockSentinel, Sentinel: SentinelOntology, Fact: "fact"},
		{Kind: LockSentinel, Sentinel: SentinelOntology, Derived: &derived},
		{Kind: LockDerived, Derived: &derived, Ref: &ref},
		{Kind: LockDerived, Derived: &derived, Fact: "fact"},
		{Kind: LockDerived, Derived: &derived, Sentinel: SentinelOntology},
		{Kind: LockSentinel, Sentinel: "grant"},
		{Kind: LockDerived, Derived: &DerivedRowKey{Table: "unknown", Key: "row"}},
	}
	for i, key := range keys {
		if err := key.Validate(); err == nil {
			t.Errorf("invalid key %d accepted: %#v", i, key)
		}
		if _, err := EncodeLockKey(key); err == nil {
			t.Errorf("invalid key %d encoded", i)
		}
		if err := ValidateLockSet(LockSet{{Key: key, Mode: LockUpdate}}); err == nil {
			t.Errorf("invalid key %d accepted in lock set", i)
		}
	}
}

func TestReadFactsAndDerivedRows(t *testing.T) {
	for _, raw := range []string{"bead/miss", "predicate:a:b", "read fact"} {
		if got, err := NewReadFact(raw); err != nil || string(got) != raw {
			t.Errorf("read fact %q = %q, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"", " fact", "fact ", "\tfact", "fact\x00"} {
		if _, err := NewReadFact(raw); err == nil {
			t.Errorf("invalid read fact %q accepted", raw)
		}
		if err := (DerivedRowKey{Table: DerivedReadyState, Key: raw}).Validate(); err == nil {
			t.Errorf("invalid derived row key %q accepted", raw)
		}
	}
	for _, table := range []DerivedTable{DerivedReadyState, DerivedAncestorClosure, DerivedCutClosure, DerivedAttribute, DerivedSortKey, DerivedReferenceIndex, DerivedGrant, DerivedOntology, DerivedPlugins} {
		key := LockKey{Kind: LockDerived, Derived: &DerivedRowKey{Table: table, Key: "row:key/1"}}
		if _, err := EncodeLockKey(key); err != nil {
			t.Errorf("derived family %q: %v", table, err)
		}
	}
}

func TestLockModesAndExactDeclaration(t *testing.T) {
	for _, mode := range []LockMode{LockShare, LockUpdate, LockSkipLocked} {
		if err := ValidateLockSet(LockSet{{Key: OntologySentinelLock, Mode: mode}}); err != nil {
			t.Errorf("valid mode %q: %v", mode, err)
		}
	}
	for _, mode := range []LockMode{"", "exclusive", "skiplocked"} {
		if err := ValidateLockSet(LockSet{{Key: OntologySentinelLock, Mode: mode}}); err == nil {
			t.Errorf("invalid mode %q accepted", mode)
		}
	}
	a := LockRequest{Key: OntologySentinelLock, Mode: LockShare}
	b := LockRequest{Key: PluginsSentinelLock, Mode: LockUpdate}
	tests := []struct {
		name     string
		declared []LockKey
		acquired LockSet
		valid    bool
	}{
		{"empty", nil, nil, true},
		{"exact", []LockKey{a.Key, b.Key}, LockSet{a, b}, true},
		{"missing", []LockKey{a.Key}, LockSet{a, b}, false},
		{"extra", []LockKey{a.Key, b.Key}, LockSet{a}, false},
		{"different", []LockKey{a.Key}, LockSet{b}, false},
		{"declaration reversed", []LockKey{b.Key, a.Key}, LockSet{a, b}, false},
		{"acquisition reversed", []LockKey{a.Key, b.Key}, LockSet{b, a}, false},
		{"duplicate declaration", []LockKey{a.Key, a.Key}, LockSet{a, b}, false},
		{"duplicate across modes", []LockKey{a.Key}, LockSet{a, {Key: a.Key, Mode: LockUpdate}}, false},
		{"invalid mode", []LockKey{a.Key}, LockSet{{Key: a.Key, Mode: "bad"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := append(LockSet(nil), tt.acquired...)
			if err := ValidateHeaderLocks(tt.declared, tt.acquired); (err == nil) != tt.valid {
				t.Fatalf("valid = %t; got %v", tt.valid, err)
			}
			if !reflect.DeepEqual(before, tt.acquired) {
				t.Fatal("validation mutated acquisition")
			}
		})
	}
}

func TestLockIdentityPreservesRefArmsAndCanonicalBranches(t *testing.T) {
	refs := []struct {
		ref  Ref
		want string
	}{
		{model.RefByID("", model.Natural64(0)), "0:id::0"},
		{model.RefByKey(""), "0:key:"},
		{model.RefByID("a:b", model.Natural("1e0")), "0:id:a:b:1"},
		{model.RefByKey("a:b:1"), "0:key:a:b:1"},
		{model.RefByID("a", model.Natural("18446744073709551616")), "0:id:a:18446744073709551616"},
	}
	for _, tt := range refs {
		key := LockKey{Kind: LockRecord, Ref: &tt.ref}
		got, err := EncodeLockKey(key)
		if err != nil || got != tt.want {
			t.Errorf("encoding = %q, %v; want %q", got, err, tt.want)
		}
	}
	a := model.RefByID("id", model.Natural("1"))
	b := model.RefByID("id", model.Natural("1.0"))
	locks := LockSet{
		{Key: LockKey{Kind: LockRecord, Ref: &a}, Mode: LockShare},
		{Key: LockKey{Kind: LockRecord, Ref: &b}, Mode: LockUpdate},
	}
	if err := ValidateLockSet(locks); err == nil {
		t.Fatal("equivalent numeric branches created two locks")
	}
	if err := ValidateHeaderLocks([]LockKey{locks[1].Key}, locks[:1]); err != nil {
		t.Fatalf("equivalent canonical declaration rejected: %v", err)
	}
	invalid := []Ref{
		{ID: "a", Key: "b"},
		model.RefByID("a", model.Natural("-1")),
		model.RefByID("a", model.Natural("0.5")),
	}
	for _, ref := range invalid {
		if _, err := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &ref}); err == nil {
			t.Errorf("invalid ref encoded: %#v", ref)
		}
	}
}

func TestSameKindLocksUseBytewiseEncodedOrder(t *testing.T) {
	a := model.RefByID("a", model.Natural64(10))
	b := model.RefByID("a", model.Natural64(2))
	c := model.RefByKey("a")
	// The order is bytewise encoding order, including branch encodings,
	// rather than numeric branch order, physical table order or lock mode.
	keys := []LockKey{
		{Kind: LockRecord, Ref: &a},
		{Kind: LockRecord, Ref: &b},
		{Kind: LockRecord, Ref: &c},
		{Kind: LockReadFact, Fact: "A"},
		{Kind: LockReadFact, Fact: "a"},
		{Kind: LockSentinel, Sentinel: "bead/a"},
		{Kind: LockSentinel, Sentinel: "bead/b"},
		{Kind: LockDerived, Derived: &DerivedRowKey{Table: DerivedGrant, Key: "z"}},
		{Kind: LockDerived, Derived: &DerivedRowKey{Table: DerivedReadyState, Key: "a"}},
	}
	if err := ValidateLockKeys(keys); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(keys); i++ {
		if err := ValidateLockKeys([]LockKey{keys[i], keys[i-1]}); err == nil {
			t.Errorf("reversed pair %d accepted", i)
		}
	}
}

func TestPrincipalKeyUsesEffectiveIdentity(t *testing.T) {
	user := model.PrincipalRef{Kind: "user", ID: "alice:1"}
	key, err := PrincipalKey(user)
	if err != nil || key != "user:alice:1" {
		t.Fatalf("principal key = %q, %v", key, err)
	}
	user.Caveats = map[string]any{"project": "engineering"}
	user.Sponsor = &model.PrincipalRef{Kind: "user", ID: "sponsor"}
	if got, err := PrincipalKey(user); err != nil || got != key {
		t.Fatalf("context changed principal identity: %q, %v", got, err)
	}
	for _, kind := range []string{"anonymous", "user", "agent", "service", "plugin", "system"} {
		if got, err := PrincipalKey(model.PrincipalRef{Kind: kind, ID: "same"}); err != nil || got != kind+":same" {
			t.Errorf("principal %q = %q, %v", kind, got, err)
		}
	}
	if _, err := PrincipalKey(model.PrincipalRef{Kind: "unknown", ID: "x"}); err == nil {
		t.Fatal("unknown principal kind accepted")
	}
}

func TestCheckpointKeyValidation(t *testing.T) {
	for _, key := range []string{"events/binding-1", "scope:revision:1", "state"} {
		if err := ValidateCheckpointKey(key); err != nil {
			t.Errorf("canonical key %q: %v", key, err)
		}
	}
	for _, key := range []string{"", " ", " key", "key ", "\nkey", "key\t", "key\x00suffix", "\u00a0key"} {
		if err := ValidateCheckpointKey(key); err == nil {
			t.Errorf("invalid key %q accepted", key)
		}
	}
}
