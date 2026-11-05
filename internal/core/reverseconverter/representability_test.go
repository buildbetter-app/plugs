package reverseconverter

import (
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

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

func TestClassifyKindAcceptsOnlyCanonicalMappedBeadsKinds(t *testing.T) {
	canonical := ClassifyKindWithMapping("workflow.item", func(string) (string, bool) { return "bead", true })
	if canonical.Disposition != KindRepresentable || canonical.BeadsKind != "bead" {
		t.Fatalf("canonical mapping classification=%+v", canonical)
	}
	unknown := ClassifyKindWithMapping("workflow.item", func(string) (string, bool) { return "issue", true })
	if unknown.Disposition != KindUnrepresentable || unknown.BeadsKind != "" {
		t.Fatalf("noncanonical mapping was admitted: %+v", unknown)
	}
}

func TestRollbackWindowRefusalCarriesTypedKind(t *testing.T) {
	err := RollbackWindowRefusal("workflow.item")
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != "workflow.item" {
		t.Fatalf("typed rollback refusal=%T %+v", err, err)
	}
}
