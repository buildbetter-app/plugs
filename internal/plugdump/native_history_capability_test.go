package plugdump

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLNativeHistoryRequiresPhysicalRootCapability(t *testing.T) {
	for _, id := range []string{strings.Repeat("a", 32), strings.Repeat("0", 32)} {
		t.Run(id, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			// A SQL connection (including one with no usable current session)
			// cannot turn AS OF or a declaration query into physical evidence.
			mock.ExpectClose()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			image, err := (SQLSource{DB: db}).ReadNativeSourceImage(context.Background(), id)
			var refusal ErrSourceHistoryUnavailable
			if image != nil || !errors.As(err, &refusal) || refusal.Reason != "native-physical-root-capability-unavailable:stock-sql" {
				t.Fatalf("stock SQL acquired physical capability: image=%v error=%v", image, err)
			}
		})
	}
}
