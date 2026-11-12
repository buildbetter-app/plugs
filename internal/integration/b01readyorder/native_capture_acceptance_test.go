package b01readyorder_test

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

const b01CompleteReadyFixture = "testdata/native-ready-c774-43216037385.tar.gz"
const b01CompleteReadyFixtureSHA256 = "30741ec284a9dfffadde302cf8fdccac4b51f04ee14f3cf26c1e6539d722a47f"

func TestB01CompleteNativeCaptureReadyAndPublicComparison(t *testing.T) {
	b01CompareCapturedReady(t, b01CompleteReadyFixture, b01CompleteReadyFixtureSHA256)
}

func TestB01CapturedIssueSourceFieldOmissionContract(t *testing.T) {
	in := b01ReadCapturedReady(t, b01CompleteReadyFixture, b01CompleteReadyFixtureSHA256)
	records, err := in.Archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	var actual []dumpformat.Row
	for _, record := range records {
		if record.Table == dumpformat.DurableEntityTable {
			var row dumpformat.Record
			b01CaptureDecode(t, record.Raw, &row)
			actual = append(actual, row.Source.Fields)
		}
	}
	b01CapturedIssueOmissionControls(t, actual, in.Tables["issues"].Rows)
}
