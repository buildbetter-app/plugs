package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLinkTargetKindWireAndCanonicalIdentity(t *testing.T) {
	for _, operation := range []string{"link", "unlink"} {
		for _, marker := range []string{"", `,"targetKind":"local"`, `,"targetKind":"external"`} {
			raw := `{"kind":"` + operation + `","from":{"id":"a"},"to":{"id":"external:tracker/1"},"linkKind":"blocks"` + marker + `}`
			if err := checkComponent(t, "LinkOp", []byte(raw)); err != nil {
				t.Fatal(err)
			}
			var op Op
			if err := json.Unmarshal([]byte(raw), &op); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			if op.Link.To.ID != "external:tracker/1" || strings.Contains(string(encoded), `"targetKind":"local"`) {
				t.Fatalf("target identity changed: %s", encoded)
			}
			if strings.Contains(marker, "external") != strings.Contains(string(encoded), `"targetKind":"external"`) {
				t.Fatalf("external arm lost: %s", encoded)
			}
		}
	}
	for _, value := range []string{`""`, `null`, `"future"`, `1`, `{}`} {
		raw := `{"kind":"link","from":{"id":"a"},"to":{"id":"b"},"linkKind":"blocks","targetKind":` + value + `}`
		if err := checkComponent(t, "LinkOp", []byte(raw)); err == nil {
			t.Fatalf("schema admitted %s", value)
		}
		var op Op
		if err := json.Unmarshal([]byte(raw), &op); err == nil {
			t.Fatalf("model admitted %s", value)
		}
	}
}

func TestExternalLinkTargetRejectsLocalAliasesAndBranches(t *testing.T) {
	for _, target := range []Ref{RefByKey("alias"), RefByID("", Natural64(0)), RefByID(string([]byte{0xff}), Natural64(0)), RefByID("x", Natural64(1)), {ID: "x", Key: "alias"}} {
		if err := ValidateLinkTarget(LinkTargetExternal, target); err == nil {
			t.Fatalf("external target admitted %#v", target)
		}
	}
	original := LinkOp{From: RefByID("a", Natural64(0)), To: RefByID("b", Natural64(0)), LinkKind: "blocks", TargetKind: LinkTargetLocal}
	if _, err := json.Marshal(Op{Kind: OpLink, Link: &original}); err != nil {
		t.Fatal(err)
	}
	if original.TargetKind != LinkTargetLocal {
		t.Fatal("canonicalization mutated caller")
	}
	original.TargetKind = "future"
	if _, err := json.Marshal(Op{Kind: OpLink, Link: &original}); err == nil {
		t.Fatal("unknown native kind marshaled")
	}
}
