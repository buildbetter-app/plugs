package dumpformat

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHashNamespaceIDsRetainBareAndSegmentedRoots(t *testing.T) {
	project := Project{
		Prefix:         "repro",
		IDMode:         "hash",
		HashLength:     3,
		HashNamespaces: []string{"abc", "zzzzzzzz"},
		ChildCounters:  map[string]uint64{"repro-abc-4de": 2},
	}
	for _, id := range []string{"repro-4de", "repro-abc-4de", "repro-abc-4de.1", "repro-abc-4de.2"} {
		if !ValidSourceID(id, project) {
			t.Fatalf("valid source ID refused: %q", id)
		}
	}
	if err := checkID("repro-abc-4de.2", project); err != nil {
		t.Fatalf("valid child refused: %v", err)
	}
}

func TestHashNamespaceIDsRefuseUndeclaredMalformedAndOutOfRangeRoots(t *testing.T) {
	project := Project{
		Prefix:         "repro",
		IDMode:         "hash",
		HashLength:     3,
		HashNamespaces: []string{"abc"},
		ChildCounters:  map[string]uint64{"repro-abc-4de": 2},
	}
	for _, id := range []struct {
		value       string
		validSource bool
	}{
		{value: "repro-xyz-4de"},                      // undeclared namespace
		{value: "repro-abc-4d"},                       // hash below the declared floor
		{value: "repro-abc-4de."},                     // empty child segment
		{value: "repro-abc-4de.0"},                    // zero child
		{value: "repro-abc-4de.3", validSource: true}, // child bounds are archive-only
		{value: "repro-abc--4de"},                     // empty namespace segment
		{value: "repro-abc-4de.1.0"},                  // invalid descendant
		{value: "other-abc-4de"},                      // wrong project prefix
		{value: "repro-abc-4de\x00"},                  // NUL is never an identity character
	} {
		if ValidSourceID(id.value, project) != id.validSource {
			t.Fatalf("unexpected source ID grammar result: %q", id.value)
		}
		if err := checkID(id.value, project); err == nil {
			t.Fatalf("invalid source ID passed archive validation: %q", id.value)
		}
	}
	if err := checkID("repro-abc-4de.3", project); err == nil || !strings.Contains(err.Error(), "exceeds allocator") {
		t.Fatalf("out-of-range child did not retain allocator refusal: %v", err)
	}
}

func TestHashNamespaceManifestRejectsInvalidDeclarations(t *testing.T) {
	for name, raw := range map[string]string{
		"null":        `{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":null,"last_id":0,"child_counters":{}}`,
		"empty":       `{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":[],"last_id":0,"child_counters":{}}`,
		"duplicate":   `{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":["abc","abc"],"last_id":0,"child_counters":{}}`,
		"unsorted":    `{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":["zz","abc"],"last_id":0,"child_counters":{}}`,
		"sequential":  `{"prefix":"repro","id_mode":"sequential","hash_length":0,"hash_namespaces":["abc"],"last_id":4,"child_counters":{}}`,
		"bad grammar": `{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":["abc..def"],"last_id":0,"child_counters":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var project Project
			if err := json.Unmarshal([]byte(raw), &project); err != nil {
				return
			}
			if err := validateProjects([]Project{project}); err == nil {
				t.Fatalf("invalid namespace declaration accepted: %s", name)
			}
		})
	}
}

func TestHashNamespaceManifestRejectsCaseAliasedField(t *testing.T) {
	var project Project
	err := json.Unmarshal([]byte(`{"prefix":"repro","id_mode":"hash","hash_length":3,"Hash_Namespaces":["abc"],"last_id":0,"child_counters":{}}`), &project)
	if err == nil {
		t.Fatal("case-aliased hash namespace field accepted")
	}
}
