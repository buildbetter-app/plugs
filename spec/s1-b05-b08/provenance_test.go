package s1b05b08

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"
)

const (
	pinnedReferenceCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	pinnedReferenceTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
)

//go:embed reference/provenance.json
var referenceProvenance []byte

func TestReferenceProvenanceIsTypedUntilFreshTwinsExist(t *testing.T) {
	var value struct {
		Schema string   `json:"schema"`
		Cards  []string `json:"cards"`
		Source struct {
			Commit string `json:"commit"`
			Tree   string `json:"tree"`
		} `json:"source"`
		Status     string          `json:"status"`
		Recordings json.RawMessage `json:"recordings"`
		Blocked    []struct {
			Stage string `json:"stage"`
			Code  string `json:"code"`
		} `json:"blocked"`
	}
	if err := json.Unmarshal(referenceProvenance, &value); err != nil {
		t.Fatal(err)
	}
	if value.Schema != "plug.s1-b05-b08.reference-provenance.v1" || value.Status != "blocked" {
		t.Fatalf("reference manifest lost its typed disposition: %+v", value)
	}
	if value.Source.Commit != pinnedReferenceCommit || value.Source.Tree != pinnedReferenceTree {
		t.Fatalf("reference pin changed: %+v", value.Source)
	}
	if string(bytes.TrimSpace(value.Recordings)) != "[]" || len(value.Blocked) < 2 {
		t.Fatalf("blocked reference manifest is incomplete: %+v", value)
	}
	if len(value.Cards) != 4 || value.Cards[0] != "B05" || value.Cards[3] != "B08" {
		t.Fatalf("card scope changed: %v", value.Cards)
	}
	t.Log("B05-B08 reference twins remain explicitly blocked until a pinned executable and Go+SQL clock evidence are supplied")
}
