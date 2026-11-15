package nativeprojection

import (
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

func closeTimeRecord(value any) dumpformat.Record {
	return dumpformat.Record{
		Kind: "bead",
		ID:   "pl-closed",
		Source: dumpformat.Provenance{Table: dumpformat.DurableEntityTable, Fields: map[string]any{
			"closed_at": value,
		}},
	}
}

func TestSourceCloseTimePreservesExactUTCInstant(t *testing.T) {
	got, err := SourceCloseTime(closeTimeRecord("2026-11-15T03:04:05.123456789+02:00"))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 15, 1, 4, 5, 123456789, time.UTC)
	if got == nil || !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("closed_at=%v, want exact UTC %s", got, want.Format(time.RFC3339Nano))
	}
}

func TestSourceCloseTimeRejectsInvalidStamp(t *testing.T) {
	if _, err := SourceCloseTime(closeTimeRecord("not-a-timestamp")); err == nil {
		t.Fatal("invalid closed_at stamp was accepted")
	}
}

func TestSourceCloseTimeLeavesAbsentFieldUnknown(t *testing.T) {
	if got, err := SourceCloseTime(closeTimeRecord(nil)); err != nil || got != nil {
		t.Fatalf("absent closed_at=%v err=%v, want nil", got, err)
	}
}
