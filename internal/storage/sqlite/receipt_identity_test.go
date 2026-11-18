package sqlite

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReceiptInvocationIdentityEncoding(t *testing.T) {
	commit := model.Commit{Revision: model.Natural64(1), Results: []model.OpResult{}, Warnings: []model.Warning{}}
	for _, token := range []string{"", "private-attempt"} {
		input := receiptResponse{Commit: commit, AttemptID: token}
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var output receiptResponse
		if err := json.Unmarshal(raw, &output); err != nil || !reflect.DeepEqual(input, output) {
			t.Fatalf("raw=%s output=%+v err=%v", raw, output, err)
		}
		var public model.Commit
		err = json.Unmarshal(raw, &public)
		if token == "" && (err != nil || !reflect.DeepEqual(commit, public)) {
			t.Fatalf("legacy public=%+v err=%v", public, err)
		}
		if token != "" && err == nil {
			t.Fatal("private metadata accepted as public Commit")
		}
	}
	var output receiptResponse
	if err := json.Unmarshal([]byte(`{"revision":1,"results":[],"warnings":[],"attemptID":"id","unknown":true}`), &output); err == nil {
		t.Fatal("receipt bypassed strict Commit validation")
	}
}
