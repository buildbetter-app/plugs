package driver

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestHierarchyCapabilitiesRemainOptional(t *testing.T) {
	for _, mandatory := range []reflect.Type{
		reflect.TypeOf((*Reader)(nil)).Elem(),
		reflect.TypeOf((*WriteTx)(nil)).Elem(),
	} {
		for _, method := range []string{"HierarchyChildren", "HierarchyChildrenExcluding", "PutHierarchyRecord"} {
			if _, exists := mandatory.MethodByName(method); exists {
				t.Fatalf("%s became mandatory on %s", method, mandatory.Name())
			}
		}
	}
}

func TestHierarchyExclusionRequestBoundsAndIdentity(t *testing.T) {
	ref := model.RefByID("opaque:child.7", model.Natural("0.0"))
	excluded := []model.Ref{ref, ref}
	before := append([]model.Ref(nil), excluded...)
	for _, limit := range []int{0, 1, HierarchyMaxLimit} {
		got, err := NormalizeHierarchyExcludingRequest(nil, excluded, limit)
		if err != nil || got != limit {
			t.Fatalf("exclusion request limit %d became %d: %v", limit, got, err)
		}
	}
	if !reflect.DeepEqual(excluded, before) {
		t.Fatal("request validation changed exclusion order, duplicates or identity")
	}
	if got, err := NormalizeHierarchyExcludingRequest(nil, nil, 0); err != nil || got != 0 {
		t.Fatalf("empty exclusion probe: limit=%d err=%v", got, err)
	}
	maximum := make([]model.Ref, HierarchyMaxExcluded)
	for i := range maximum {
		maximum[i] = ref
	}
	if _, err := NormalizeHierarchyExcludingRequest(nil, maximum, 0); err != nil {
		t.Fatalf("maximum exclusion vector: %v", err)
	}
	if got, err := NormalizeHierarchyExcludingRequest(nil, append(maximum, ref), 0); !errors.Is(err, ErrUnsupported) || got != 0 {
		t.Fatalf("oversized exclusions: limit=%d err=%v", got, err)
	}
	for _, bad := range []model.Ref{{}, model.RefByKey("alias"), model.RefByID("foreign", model.Natural64(1))} {
		if got, err := NormalizeHierarchyExcludingRequest(nil, []model.Ref{ref, bad}, 0); err == nil || got != 0 {
			t.Fatalf("invalid later exclusion accepted for empty probe: %+v limit=%d err=%v", bad, got, err)
		}
		if _, err := NormalizeHierarchyExcludingRequest([]model.Ref{bad}, excluded, 0); err == nil {
			t.Fatalf("exclusions bypassed parent validation: %+v", bad)
		}
	}
	for _, limit := range []int{-1, HierarchyMaxLimit + 1, int(^uint(0) >> 1)} {
		if got, err := NormalizeHierarchyExcludingRequest(nil, excluded, limit); err == nil || got != 0 {
			t.Fatalf("exclusions bypassed invalid limit %d: limit=%d err=%v", limit, got, err)
		}
	}
}

func TestHierarchyRequestBoundsAndCanonicalParents(t *testing.T) {
	parent := model.RefByID("project:opaque-9.12", model.Natural("0e2"))
	for _, tc := range []struct {
		name    string
		parents []model.Ref
		limit   int
		want    int
	}{
		{"empty_probe", nil, 0, 0},
		{"empty", nil, 1, 1},
		{"parent_probe", []model.Ref{parent}, 0, 0},
		{"duplicate_parents", []model.Ref{parent, parent}, 5001, 5001},
		{"maximum_limit", []model.Ref{parent}, HierarchyMaxLimit, HierarchyMaxLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeHierarchyRequest(tc.parents, tc.limit)
			if err != nil || got != tc.want || got+1 <= got {
				t.Fatalf("limit=%d err=%v want=%d with safe limit+1", got, err, tc.want)
			}
		})
	}
	parents := make([]model.Ref, HierarchyMaxParents)
	for i := range parents {
		parents[i] = parent
	}
	if _, err := NormalizeHierarchyRequest(parents, 1); err != nil {
		t.Fatalf("maximum vector: %v", err)
	}
	if _, err := NormalizeHierarchyRequest(append(parents, parent), 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("oversized vector error=%v", err)
	}
	for _, limit := range []int{-1, HierarchyMaxLimit + 1, int(^uint(0) >> 1)} {
		if got, err := NormalizeHierarchyRequest(nil, limit); err == nil || got != 0 {
			t.Fatalf("unsafe limit %d accepted as %d: %v", limit, got, err)
		}
	}
	bad := model.RefByKey("parent-alias")
	for _, limit := range []int{0, 1} {
		if got, err := NormalizeHierarchyRequest([]model.Ref{parent, parent, bad}, limit); err == nil || got != 0 {
			t.Fatalf("later invalid parent accepted for limit %d: limit=%d err=%v", limit, got, err)
		}
	}
}

func TestHierarchyRefEnvelopePreservesOpaqueIdentity(t *testing.T) {
	for _, id := range []string{"project-1", "project-1.2", "opaque:part:0", "λ-2"} {
		ref := model.RefByID(id, model.Natural("0.0"))
		if err := ValidateHierarchyRef(ref); err != nil {
			t.Fatalf("canonical id %q: %v", id, err)
		}
		if ref.ID != id || ref.Branch != model.Natural("0.0") {
			t.Fatal("validation normalized caller-owned identity")
		}
	}
	for _, ref := range []model.Ref{
		{},
		model.RefByID("", model.Natural64(0)),
		model.RefByKey("parent-alias"),
		model.RefByID("parent", model.Natural64(1)),
		model.RefByID("parent", model.Natural("18446744073709551616")),
		model.RefByID("parent", model.Natural("-1")),
		{ID: "parent", Key: "alias"},
		model.RefByID(" parent", model.Natural64(0)),
		model.RefByID("parent\n", model.Natural64(0)),
		model.RefByID("parent/child", model.Natural64(0)),
		model.RefByID("parent\x00child", model.Natural64(0)),
	} {
		if err := ValidateHierarchyRef(ref); err == nil {
			t.Fatalf("accepted malformed hierarchy ref %+v", ref)
		}
	}
}

func TestHierarchyRecordValidationKeepsParentAndBodyIndependent(t *testing.T) {
	child := model.RefByID("opaque-child.7", model.Natural64(0))
	parent := model.RefByID("unrelated:canonical-parent", model.Natural("0.0"))
	body := []byte("{ \n \"large\": 9007199254740993, \"parent\": {\"opaque\":true}, \"escaped\":\"\\u0061\" }")
	before := bytes.Clone(body)
	record := Record{Ref: child, Kind: "bead", Revision: model.Natural64(7), Body: body}
	beforeParent := parent
	// Only core can establish the meaning/agreement of the JSON Parent. This
	// helper checks the typed envelope without decoding or rewriting that JSON.
	if err := ValidateHierarchyRecord(record, &parent); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHierarchyRecord(record, nil); err != nil {
		t.Fatalf("explicit root refused: %v", err)
	}
	if !bytes.Equal(record.Body, before) || !reflect.DeepEqual(parent, beforeParent) || record.Revision.String() != "7" {
		t.Fatal("validation changed source bytes, parent or revision")
	}
	if err := ValidateHierarchyRecord(record, &child); err == nil {
		t.Fatal("self parent accepted")
	}
	for _, tc := range []struct {
		name   string
		record Record
		parent *model.Ref
	}{
		{"non_bead", Record{Ref: child, Kind: "plugin", Body: body}, nil},
		{"missing_kind", Record{Ref: child, Body: body}, nil},
		{"malformed_body", Record{Ref: child, Kind: "bead", Body: []byte("{")}, nil},
		{"missing_body", Record{Ref: child, Kind: "bead"}, nil},
		{"alias_child", Record{Ref: model.RefByKey("alias"), Kind: "bead", Body: body}, nil},
		{"foreign_child", Record{Ref: model.RefByID(child.ID, model.Natural64(1)), Kind: "bead", Body: body}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateHierarchyRecord(tc.record, tc.parent); err == nil {
				t.Fatal("malformed source envelope accepted")
			}
		})
	}
	for _, badParent := range []model.Ref{model.RefByKey("alias"), model.RefByID("parent", model.Natural64(1)), model.RefByID("", model.Natural64(0))} {
		if err := ValidateHierarchyRecord(record, &badParent); err == nil {
			t.Fatalf("malformed parent accepted: %+v", badParent)
		}
	}
}
