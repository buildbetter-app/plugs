package cardb01

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

const (
	pinnedBeadsCommit      = "9d6fff760b7a314b476539198504d77ce0959656"
	pinnedBeadsTree        = "cde121d6e4119e454857ad1c3ca293e2791be840"
	referenceBinarySHA256  = "b4b880c41df1d21f7802d1a7d2dc0c4b85ff36fed86e9758a3740045e9590365"
	referenceVersionDigest = "d866eca35c70693209f7a4f6f8c6d0f2f9e44ff481f15623711cadb91c51317f"
	recordingSHA256        = "088e187096cd33de2fa83f656f0b0bc279f3b66c9bd1f31da04613c060c8bbc4"
)

//go:embed reference-run-1.json
var referenceRun1 []byte

//go:embed reference-run-2.json
var referenceRun2 []byte

//go:embed record_reference.py
var referenceRecorder []byte

type referenceRecording struct {
	Card           string          `json:"card"`
	Scope          string          `json:"scope"`
	Engine         string          `json:"engine"`
	SourceCommit   string          `json:"sourceCommit"`
	SourceTree     string          `json:"sourceTree"`
	Binary         binaryEvidence  `json:"binary"`
	RecorderSHA256 string          `json:"recorderSha256"`
	Configuration  map[string]any  `json:"configuration"`
	FixtureDigest  string          `json:"fixtureDigest"`
	Raw            json.RawMessage `json:"raw"`
	RawDigest      string          `json:"rawDigest"`
	Projection     []struct {
		CreatedAt string `json:"createdAt"`
		ID        string `json:"id"`
		Priority  int    `json:"priority"`
		Status    string `json:"status"`
	} `json:"projection"`
	ProjectionDigest string   `json:"projectionDigest"`
	ExcludedIDs      []string `json:"excludedIDs"`
	Limits           []string `json:"limits"`
}

type binaryEvidence struct {
	SHA256        string   `json:"sha256"`
	VersionDigest string   `json:"versionDigest"`
	BuildTags     []string `json:"buildTags"`
}

func sha(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func TestB01FreshReferenceRecordingsArePinnedAndIdentical(t *testing.T) {
	if !bytes.Equal(referenceRun1, referenceRun2) {
		t.Fatal("independent fresh Beads recordings differ")
	}
	if got := sha(referenceRun1); got != recordingSHA256 {
		t.Fatalf("retained recording SHA-256 = %s, want %s", got, recordingSHA256)
	}
	var got referenceRecording
	if err := json.Unmarshal(referenceRun1, &got); err != nil {
		t.Fatal(err)
	}
	if got.Card != "B01" || got.Scope != "non-temporal-ready-membership-priority-id-tie" || got.Engine != "embedded-dolt" {
		t.Fatalf("reference identity changed: %+v", got)
	}
	if got.SourceCommit != pinnedBeadsCommit || got.SourceTree != pinnedBeadsTree {
		t.Fatalf("source provenance changed: commit=%s tree=%s", got.SourceCommit, got.SourceTree)
	}
	if got.Binary.SHA256 != referenceBinarySHA256 || got.Binary.VersionDigest != referenceVersionDigest ||
		!reflect.DeepEqual(got.Binary.BuildTags, []string{"gms_pure_go"}) {
		t.Fatalf("binary provenance is incomplete: %+v", got.Binary)
	}
	if got.RecorderSHA256 != sha(referenceRecorder) {
		t.Fatalf("recorder source digest = %s, actual %s", got.RecorderSHA256, sha(referenceRecorder))
	}
	if got.RawDigest != sha(got.Raw) {
		t.Fatalf("raw projection digest = %s, actual %s", got.RawDigest, sha(got.Raw))
	}
	projection, err := json.Marshal(got.Projection)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProjectionDigest != sha(projection) {
		t.Fatalf("semantic projection digest = %s, actual %s", got.ProjectionDigest, sha(projection))
	}
	ids := make([]string, len(got.Projection))
	for i, item := range got.Projection {
		ids[i] = item.ID
		if item.Status != "open" || item.CreatedAt != "2026-01-01T00:00:00Z" {
			t.Fatalf("reference projection was normalized beyond selected stable fields: %+v", item)
		}
	}
	if !reflect.DeepEqual(ids, []string{"b01-p0", "b01-tie-a", "b01-tie-b"}) ||
		!reflect.DeepEqual(got.ExcludedIDs, []string{"b01-closed", "b01-deferred", "b01-pinned"}) {
		t.Fatalf("reference membership/order changed: included=%v excluded=%v", ids, got.ExcludedIDs)
	}
	if len(got.Limits) != 3 {
		t.Fatalf("reference evidence limits are missing: %v", got.Limits)
	}
	t.Log("B01 reference: two fresh pinned embedded-Dolt runs were byte-identical for the selected non-temporal membership and explicit priority/ID-tie projection")
}
