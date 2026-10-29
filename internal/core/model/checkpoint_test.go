package model

import (
	"encoding/json"
	"testing"
)

func TestCheckpoint(t *testing.T) {
	var cs ChangeSet
	if err := json.Unmarshal([]byte(`{"ops":[{"kind":"create","key":"new","type":"task","fields":{"title":"first"}},{"kind":"comment","target":{"key":"new"},"body":"hello"}]}`), &cs); err != nil {
		t.Fatal(err)
	}
	if len(cs.Ops) != 2 || cs.Ops[0].Create == nil || cs.Ops[1].Comment == nil {
		t.Fatal("operation arms lost")
	}
	if _, err := json.Marshal(cs); err != nil {
		t.Fatal(err)
	}
	r := RefByID("", Natural64(0))
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if !r.IsID() || r.Branch.String() != "0" {
		t.Fatal("reference arm or numeric zero lost")
	}
	var revision Revision
	if err := json.Unmarshal([]byte(`184467440737095516160`), &revision); err != nil {
		t.Fatal(err)
	}
	if _, err := revision.Uint64(); err == nil {
		t.Fatal("overflow accepted")
	}
}
