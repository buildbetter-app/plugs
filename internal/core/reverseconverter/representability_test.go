package reverseconverter

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReverseRegistryRequiresExecutableContentMapping(t *testing.T) {
	identity := func(r Record) (Record, error) { return r, nil }
	for _, mappings := range [][]RecordMapping{
		{{Kind: "native.note", BeadsKind: "memory"}},
		{{Kind: "native.note", BeadsKind: "unknown", Convert: identity}},
		{{Kind: "native.note", BeadsKind: "beads-archive", Convert: identity}},
		{{Kind: "bead", BeadsKind: "memory", Convert: identity}},
		{{Kind: "workflow.receipts", BeadsKind: "memory", Convert: identity}},
		{{Kind: "native.note", BeadsKind: "memory", Convert: identity}, {Kind: "native.note", BeadsKind: "memory", Convert: identity}},
	} {
		if _, err := NewConverter(mappings...); err == nil {
			t.Fatalf("invalid reverse registration accepted: %+v", mappings)
		}
	}
}

func TestRegisteredKindAdmissionExecutesActualReverseConversion(t *testing.T) {
	dump := fixtureDump()
	// Keep the complete canonical memory shape but move it to a new native
	// kind. Only the registered executable transformation can convert it back.
	record := dump.Records["memory"][0]
	record.Kind = "native.note"
	dump.Records["native.note"] = []Record{record}
	delete(dump.Records, "memory")
	for i, kind := range dump.Manifest.Kinds {
		if kind.Name == "memory" {
			dump.Manifest.Kinds[i] = Kind{Name: "native.note", Version: 1, File: "records/native.note.ndjson", Count: 1}
		}
	}
	// native.note is last in the already sorted registry.
	beforeDigest := digestDump(dump)
	if classification := ClassifyKind("native.note"); classification.Disposition != KindUnrepresentable {
		t.Fatalf("unregistered classification=%+v", classification)
	}
	if result, err := Convert(dump); err == nil || len(result.JSONL) != 0 {
		t.Fatalf("unregistered conversion=%+v err=%v", result, err)
	}
	called := 0
	converter, err := NewConverter(RecordMapping{Kind: "native.note", BeadsKind: "memory", Convert: func(record Record) (Record, error) {
		called++
		record.Kind = "memory"
		return record, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if classification := converter.ClassifyKind("native.note"); classification.Disposition != KindRepresentable || classification.BeadsKind != "memory" {
		t.Fatalf("registered classification=%+v", classification)
	}
	converted, err := converter.Convert(dump)
	if err != nil || called != 1 || !bytes.Contains(converted.JSONL, []byte(`"key":"fixture","value":"memory value"`)) {
		t.Fatalf("registered conversion did not emit content: called=%d result=%s err=%v", called, converted.JSONL, err)
	}
	if converted.Report.InputDigest != beforeDigest || digestDump(dump) != beforeDigest || !reflect.DeepEqual(dump.Records["native.note"][0], record) {
		t.Fatal("registered conversion changed source identity or input digest")
	}
	// A callback cannot bypass source census or canonical output validation.
	dump.Manifest.Kinds[len(dump.Manifest.Kinds)-1].Count++
	if result, err := converter.Convert(dump); err == nil || len(result.JSONL) != 0 || called != 1 {
		t.Fatalf("malformed source reached mapping: called=%d result=%+v err=%v", called, result, err)
	}
	dump.Manifest.Kinds[len(dump.Manifest.Kinds)-1].Count--
	for _, convert := range []func(Record) (Record, error){
		func(record Record) (Record, error) { return record, nil },
		func(record Record) (Record, error) {
			record.Kind = "memory"
			record.Body["unsupported"] = true
			return record, nil
		},
	} {
		bad, err := NewConverter(RecordMapping{Kind: "native.note", BeadsKind: "memory", Convert: convert})
		if err != nil {
			t.Fatal(err)
		}
		result, err := bad.Convert(dump)
		var refusal *Refusal
		if !errors.As(err, &refusal) || len(result.JSONL) != 0 || refusal.Report.InputDigest != beforeDigest || digestDump(dump) != beforeDigest {
			t.Fatalf("invalid mapped output escaped canonical validation: result=%+v err=%v", result, err)
		}
	}
}

func TestClassifyKindUsesCanonicalVocabularyAndOperationalLoss(t *testing.T) {
	for _, test := range []struct {
		kind        string
		disposition KindDisposition
		beadsKind   string
	}{
		{kind: "bead", disposition: KindRepresentable, beadsKind: "bead"},
		{kind: "memory", disposition: KindRepresentable, beadsKind: "memory"},
		{kind: "beads-archive", disposition: KindOperational},
		{kind: "workflow.run", disposition: KindUnrepresentable},
		{kind: "workflow.item", disposition: KindUnrepresentable},
	} {
		got := ClassifyKind(test.kind)
		if got.Disposition != test.disposition || got.BeadsKind != test.beadsKind {
			t.Fatalf("kind %q classification=%+v, want disposition=%q beads=%q", test.kind, got, test.disposition, test.beadsKind)
		}
	}
}

func TestRollbackWindowRefusalCarriesTypedKind(t *testing.T) {
	err := RollbackWindowRefusal("workflow.item")
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != "workflow.item" {
		t.Fatalf("typed rollback refusal=%T %+v", err, err)
	}
}
