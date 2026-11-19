package postgres

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReceiptInvocationIdentityEncoding(t *testing.T) {
	for _, token := range []string{"", "private-attempt"} {
		input := driver.IdempotencyRecord{AttemptID: token, RequestHash: "same-request", Commit: model.Commit{Revision: model.Natural64(1), Results: []model.OpResult{}, Warnings: []model.Warning{}}}
		raw, err := encodeReceipt(input)
		if err != nil {
			t.Fatal(err)
		}
		output, err := decodeReceipt(raw)
		if err != nil || !reflect.DeepEqual(input, output) {
			t.Fatalf("raw=%s output=%+v err=%v", raw, output, err)
		}
	}
}
