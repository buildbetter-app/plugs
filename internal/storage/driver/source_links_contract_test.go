package driver

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestSourceLinkCapabilitiesRemainOptional(t *testing.T) {
	for _, mandatory := range []reflect.Type{reflect.TypeOf((*Reader)(nil)).Elem(), reflect.TypeOf((*WriteTx)(nil)).Elem()} {
		for _, method := range []string{"PutGraphRecord", "InverseSourceLinks"} {
			if _, exists := mandatory.MethodByName(method); exists {
				t.Fatalf("%s became mandatory on %s", method, mandatory.Name())
			}
		}
	}
}

func TestInverseSourceLinksRequestClosedDomainAndBounds(t *testing.T) {
	ref := model.RefByID("opaque:target.7", model.Natural("0.0"))
	kinds := []IncomingLinkKind{IncomingLinkParentChild, IncomingLinkBlocks, IncomingLinkConditionalBlocks, IncomingLinkWaitsFor}
	before := append([]IncomingLinkKind(nil), kinds...)
	for _, limit := range []int{0, 1, SourceLinkMaxLimit} {
		if got, err := NormalizeInverseSourceLinksRequest([]model.Ref{ref, ref}, kinds, []model.Ref{ref}, limit); err != nil || got != limit {
			t.Fatalf("limit %d became %d: %v", limit, got, err)
		}
	}
	if !reflect.DeepEqual(kinds, before) {
		t.Fatal("normalization changed requested kind order or identity")
	}
	if got, err := NormalizeInverseSourceLinksRequest(nil, nil, nil, 0); err != nil || got != 0 {
		t.Fatalf("empty probe: limit=%d err=%v", got, err)
	}
	if _, err := NormalizeInverseSourceLinksRequest([]model.Ref{ref}, []IncomingLinkKind{IncomingLinkBlocks, IncomingLinkBlocks}, nil, 1); err != nil {
		t.Fatalf("duplicate requested kind: %v", err)
	}
	for _, kind := range []IncomingLinkKind{"", "related", "custom", " blocks", "blocks\x00"} {
		if _, err := NormalizeInverseSourceLinksRequest(nil, []IncomingLinkKind{IncomingLinkBlocks, kind}, nil, 0); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("uncovered later kind %q accepted for empty probe: %v", kind, err)
		}
	}
	if _, err := NormalizeInverseSourceLinksRequest(nil, append(kinds, IncomingLinkBlocks), nil, 0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("oversized kind vector: %v", err)
	}
	targets := make([]model.Ref, SourceLinkMaxTargets)
	excluded := make([]model.Ref, SourceLinkMaxExcluded)
	for i := range targets {
		targets[i] = ref
	}
	for i := range excluded {
		excluded[i] = ref
	}
	if _, err := NormalizeInverseSourceLinksRequest(targets, kinds, excluded, 0); err != nil {
		t.Fatalf("maximum request vectors: %v", err)
	}
	if _, err := NormalizeInverseSourceLinksRequest(append(targets, ref), kinds, excluded, 0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("oversized targets: %v", err)
	}
	if _, err := NormalizeInverseSourceLinksRequest(targets, kinds, append(excluded, ref), 0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("oversized exclusions: %v", err)
	}
	bad := model.RefByKey("alias")
	if _, err := NormalizeInverseSourceLinksRequest([]model.Ref{ref, bad}, nil, nil, 0); err == nil {
		t.Fatal("empty kind selection bypassed target validation")
	}
	if _, err := NormalizeInverseSourceLinksRequest(nil, nil, []model.Ref{ref, bad}, 0); err == nil {
		t.Fatal("empty selection bypassed exclusion validation")
	}
	for _, limit := range []int{-1, SourceLinkMaxLimit + 1, int(^uint(0) >> 1)} {
		if got, err := NormalizeInverseSourceLinksRequest(nil, kinds, nil, limit); err == nil || got != 0 {
			t.Fatalf("invalid limit %d became %d: %v", limit, got, err)
		}
	}
}

func TestGraphSourceMetadataPreservesOpaqueAllKindDeclaration(t *testing.T) {
	child := model.RefByID("opaque:source.7", model.Natural64(0))
	parent := model.RefByID("parent", model.Natural64(0))
	target := model.RefByID("unrelated:target", model.Natural("0.0"))
	body := []byte("{\n \"large\":9007199254740993,\"links\":{\"opaque\":true},\"escaped\":\"\\u0061\" }")
	record := Record{Ref: child, Kind: "bead", Revision: model.Natural64(7), Body: body}
	beforeBody := bytes.Clone(body)
	links := []SourceLink{
		{Kind: "parent-child", To: target},
		{Kind: "blocks", To: target},
		{Kind: "conditional-blocks", To: target},
		{Kind: "waits-for", To: target},
		{Kind: "related", To: target},
		{Kind: " custom/\x00kind ", To: target},
		{Kind: strings.Repeat("x", 32), To: target},
		{Kind: strings.Repeat("é", 16), To: target},
		{Kind: "blocks", To: target},
	}
	beforeLinks := append([]SourceLink(nil), links...)
	beforeParent := parent
	// Only core establishes JSON semantics and completeness. Storage accepts
	// the exact declared envelope without interpreting or rewriting Body.
	if err := ValidateGraphSourceRecord(record, &parent, links); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGraphSourceRecord(record, nil, nil); err != nil {
		t.Fatalf("explicit root and empty outgoing set: %v", err)
	}
	if !bytes.Equal(record.Body, beforeBody) || !reflect.DeepEqual(links, beforeLinks) || !reflect.DeepEqual(parent, beforeParent) || record.Revision.String() != "7" {
		t.Fatal("validation changed the source image or supplied metadata")
	}
	for _, kind := range []string{"", strings.Repeat("x", 33), strings.Repeat("é", 17), string([]byte{0xff})} {
		if err := ValidateGraphSourceRecord(record, &parent, []SourceLink{links[0], {Kind: kind, To: target}}); err == nil {
			t.Fatalf("invalid later outgoing kind %q accepted", kind)
		}
	}
	for _, bad := range []model.Ref{{}, model.RefByKey("alias"), model.RefByID("foreign", model.Natural64(1)), model.RefByID("non/canonical", model.Natural64(0))} {
		if err := ValidateGraphSourceRecord(record, &parent, []SourceLink{links[0], {Kind: "blocks", To: bad}}); err == nil {
			t.Fatalf("invalid later outgoing target accepted: %+v", bad)
		}
	}
	if err := ValidateGraphSourceRecord(Record{Ref: child, Kind: "memory", Body: body}, nil, nil); err == nil {
		t.Fatal("graph publication accepted a non-bead")
	}
	if err := ValidateGraphSourceRecord(Record{Ref: child, Kind: "bead", Body: []byte("{")}, nil, nil); err == nil {
		t.Fatal("graph publication accepted malformed source bytes")
	}
}
