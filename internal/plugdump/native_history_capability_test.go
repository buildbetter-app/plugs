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

// An unavailable physical reader must retain its actual typed cause at Build's
// boundary instead of becoming a generic absent-head classification.
type refusedNativeHead struct {
	nativeHistoryFixture
	cause error
	reads []string
}

func (s *refusedNativeHead) ReadNativeSourceImage(_ context.Context, id string) (map[string]Table, error) {
	s.reads = append(s.reads, id)
	return nil, s.cause
}

func TestBuildNativeHistoryPreservesPhysicalHeadRefusal(t *testing.T) {
	fixture, inventory := nativeHistoryFixtureForTest(t)
	cause := ErrSourceHistoryUnavailable{Reason: "native-physical-root-capability-unavailable:stock-sql"}
	source := &refusedNativeHead{nativeHistoryFixture: fixture, cause: cause}
	archive, err := Build(context.Background(), source, inventory)
	var actual ErrSourceHistoryUnavailable
	if len(archive) != 0 || !errors.As(err, &actual) || actual.Reason != cause.Reason {
		t.Fatalf("physical capability cause lost or archive published: bytes=%d error=%v", len(archive), err)
	}
	if len(source.reads) != 1 || source.reads[0] != fixture.log.Head {
		t.Fatalf("unavailable HEAD was treated as historical coverage: reads=%v", source.reads)
	}
}
