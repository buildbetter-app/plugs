package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

type countedArchiveReader struct{ remaining, read int64 }

func (r *countedArchiveReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(buffer))
	if n > r.remaining {
		n = r.remaining
	}
	for i := range buffer[:int(n)] {
		buffer[i] = 'x'
	}
	r.remaining -= n
	r.read += n
	return int(n), nil
}

func TestProducedArchiveReadCapAndGrowthNeverReturnAnOversizePrefix(t *testing.T) {
	const cap = int64(1024)
	for _, size := range []int64{cap - 1, cap, cap + 1, 1 << 40} {
		reader := &countedArchiveReader{remaining: size}
		data, err := readBoundedBeadsArchive(reader, cap)
		if size <= cap {
			if err != nil || int64(len(data)) != size || reader.read != size || !bytes.Equal(data, bytes.Repeat([]byte("x"), int(size))) {
				t.Fatal("bounded accepted bytes changed")
			}
		} else if err == nil || data != nil || reader.read != cap+1 {
			t.Fatal("oversize read escaped the cap or returned prefix bytes for a whole-file digest")
		}
	}
}

func TestOversizedProducedFileRefusesWithoutAcceptedOrTruncatedDigest(t *testing.T) {
	output, source, submitted, result := originalReadbackControl(t, false)
	// An actual sparse file exercises the production 128MiB bound without
	// materializing its declared size. The streaming cap also has a separate
	// counted-reader control for growth after the file size observation.
	if err := os.Truncate(filepath.Join(output, "beads-raw-readback.tar"), dumpformat.MaxArchiveBytes+1); err != nil {
		t.Fatal(err)
	}
	assessBeadsReadback(output, source, submitted, &result)
	if result.Status != "BLOCK" || result.BeadsReadback != "FAIL" || result.Reason != "beads_raw_readback_invalid" || result.BeadsReadbackSHA256 != "" || len(result.SourceFieldLosses) != 0 {
		t.Fatalf("oversize archive acquired acceptance: %+v", result)
	}
	refusal := readPrivateRefusal(t, output, result)
	if refusal.Validation != "FAIL" || refusal.NativeComparison != "NOT_MEASURED" || refusal.AcceptedSHA256 != "" || refusal.ProducedSHA256 != "" || refusal.ValidationError != "archive exceeds 134217728 bytes" || len(refusal.CellRefusals) != 0 || len(refusal.FieldRows) != 0 {
		t.Fatalf("oversize prefix was certified as produced bytes or native evidence: %+v", refusal)
	}
}

// The archived bytes were produced by the original public 8f19 run. These
// tests replay only the consumer boundary; no executable or native result is
// substituted, and no fresh native execution is claimed.
func originalReadbackControl(t *testing.T, partial bool) (string, *dumpformat.Archive, []byte, outcome) {
	t.Helper()
	leaf, sourcePath := "memory-complete", "public-complete-root-990eb800.tar"
	if partial {
		leaf, sourcePath = "memory-partial", "public-partial-990eb800.tar"
	}
	dir := "testdata/readback-8f19"
	var provenance struct {
		Head        string `json:"head"`
		OriginalRun string `json:"original_run"`
		Files       map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int    `json:"size"`
		} `json:"files"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "provenance.json"))
	if err != nil || json.Unmarshal(raw, &provenance) != nil || provenance.Head != "8f19f328a85572ab58df5541ffbef7dea57d4837" || provenance.OriginalRun != "42797608302" {
		t.Fatal("original public control binding changed")
	}
	read := func(name string) []byte {
		t.Helper()
		name = leaf + "-" + name
		data, err := os.ReadFile(filepath.Join(dir, name))
		binding, ok := provenance.Files[name]
		if err != nil || !ok || len(data) != binding.Size || hash(data) != binding.SHA256 {
			t.Fatal("original public bytes changed")
		}
		return data
	}
	output := t.TempDir()
	if err := writePrivate(output, "beads-raw-readback.tar", read("beads-raw-readback.tar")); err != nil {
		t.Fatal(err)
	}
	source, err := dumpformat.ReadFile("../../internal/integration/converterroundtrip/testdata/" + sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	var result outcome
	if err := json.Unmarshal(read("outcome.json"), &result); err != nil || result.ArchiveSHA256 != source.SHA256() {
		t.Fatal("original source binding changed")
	}
	result.SourceFieldLosses, result.BeadsReadbackSHA256, result.BeadsRefusalSHA256 = nil, "", ""
	return output, source, read("reverse-live.jsonl"), result
}

func readPrivateRefusal(t *testing.T, dir string, result outcome) beadsReadbackRefusal {
	t.Helper()
	path := filepath.Join(dir, "beads-readback-refusal.json")
	data, err := os.ReadFile(path)
	if err != nil || result.BeadsRefusalSHA256 == "" || hash(data) != result.BeadsRefusalSHA256 {
		t.Fatal("private refusal digest missing or changed")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("refusal is not owner-only")
	}
	var refusal beadsReadbackRefusal
	if err := decodeNumberJSON(data, &refusal); err != nil {
		t.Fatal(err)
	}
	public, err := json.Marshal(result)
	if err != nil || bytes.Contains(public, []byte("demo-42v")) || bytes.Contains(public, []byte("edge-fixture")) || bytes.Contains(public, []byte("9007199254740993")) || bytes.Contains(public, []byte("envelope identity mismatch")) {
		t.Fatal("public outcome disclosed private row/cell details")
	}
	return refusal
}

func TestOriginalCompleteReadbackRetainsEveryTypedCellRefusal(t *testing.T) {
	output, source, submitted, result := originalReadbackControl(t, false)
	assessBeadsReadback(output, source, submitted, &result)
	if result.Status != "BLOCK" || result.BeadsReadback != "FAIL" || result.Reason != "beads_raw_field_changed" || len(result.SourceFieldLosses) != 8 || result.BeadsReadbackSHA256 == "" {
		t.Fatalf("original native losses were concealed: %+v", result)
	}
	refusal := readPrivateRefusal(t, output, result)
	if refusal.Validation != "PASS" || refusal.NativeComparison != "MEASURED" || refusal.AcceptedSHA256 != result.BeadsReadbackSHA256 || refusal.ProducedSHA256 != refusal.AcceptedSHA256 || refusal.SourceSHA256 != source.SHA256() || refusal.SubmittedSHA256 != hash(submitted) || refusal.Identity != nil || len(refusal.CellRefusals) != 9 || len(refusal.FieldRows) != 9 || refusal.ImporterCommit != beadsCommit || refusal.CaptureCommit != publicCaptureCommit || refusal.ImporterBinarySHA256 != result.BeadsBinarySHA256 || refusal.CaptureBinarySHA256 != result.BeadsCaptureBinarySHA256 {
		t.Fatalf("measured boundary binding incomplete: %+v", refusal)
	}
	cells := map[string]readbackCellRefusal{}
	for _, cell := range refusal.CellRefusals {
		if cell.Row != "demo-42v" || !cell.Expected.Present || !cell.Actual.Present {
			t.Fatal("cell lost its row or presence")
		}
		cells[cell.Domain+"/"+cell.Field] = cell
	}
	for _, field := range []string{"agent_state", "hook_bead", "role_bead", "role_type"} {
		c := cells["native/"+field]
		if c.Expected.Type != "null" || c.Expected.Value != nil || c.Actual.Type != "string" || c.Actual.Value != "" {
			t.Fatalf("NULL loss changed: %+v", c)
		}
	}
	for _, field := range []string{"ephemeral", "is_template"} {
		c := cells["native/"+field]
		if c.Expected.Type != "string" || c.Expected.Value != "0" || c.Actual.Type != "number" || c.Actual.Value != json.Number("0") {
			t.Fatalf("TEXT flag loss changed: %+v", c)
		}
	}
	c := cells["native/payload"]
	payload, ok := c.Expected.Value.(map[string]any)
	if !ok || c.Expected.Type != "object" || len(payload) != 2 || payload["n"] != json.Number("9007199254740992") || payload["null"] != nil || c.Actual.Type != "string" || c.Actual.Value != "" {
		t.Fatalf("structured payload loss changed: %+v", c)
	}
	for _, test := range []struct{ key, got string }{{"native/timeout_ns", "0"}, {"metadata/beads.timeout_ns", "9007199254740992"}} {
		c := cells[test.key]
		if c.Expected.Type != "number" || c.Expected.Value != json.Number("9007199254740993") || c.Actual.Type != "number" || c.Actual.Value != json.Number(test.got) {
			t.Fatalf("wide number refusal rounded in owned evidence: %+v", c)
		}
	}
	for _, count := range refusal.FieldRows {
		if count.Rows != 1 {
			t.Fatal("refusal row count changed")
		}
	}
}

func TestOriginalPartialReadbackRetainsTypedIdentityRefusalWithoutAcceptance(t *testing.T) {
	output, source, submitted, result := originalReadbackControl(t, true)
	assessBeadsReadback(output, source, submitted, &result)
	if result.Status != "BLOCK" || result.Reason != "beads_raw_readback_invalid" || result.BeadsReadback != "FAIL" || result.BeadsReadbackSHA256 != "" || len(result.SourceFieldLosses) != 0 {
		t.Fatalf("invalid archive acquired comparison credit: %+v", result)
	}
	refusal := readPrivateRefusal(t, output, result)
	if refusal.Validation != "FAIL" || refusal.NativeComparison != "NOT_MEASURED" || refusal.AcceptedSHA256 != "" || refusal.ProducedSHA256 == "" || len(refusal.CellRefusals) != 0 || len(refusal.FieldRows) != 0 || refusal.ValidationError != "source plane dependency metadata: edge metadata envelope identity mismatch" || refusal.Identity == nil {
		t.Fatalf("invalid archive was projected or lost its refusal: %+v", refusal)
	}
	id := refusal.Identity
	if id.Code != "edge_metadata_identity_mismatch" || id.Table != dumpformat.EphemeralDependencyTable || id.Field != "metadata._plug_edge_id" || id.RawID != "f589ea2e-9ce6-5427-abb0-e7ff1c64eb6b" || id.Envelope.Type != "string" || !id.Envelope.Present || id.Envelope.Value != "edge-fixture" {
		t.Fatalf("actual rejected identities changed: %+v", id)
	}
}

func TestReadbackRefusalEvidenceCannotOverwriteOrClaimAnUnwrittenDigest(t *testing.T) {
	for _, partial := range []bool{false, true} {
		output, source, submitted, result := originalReadbackControl(t, partial)
		path := filepath.Join(output, "beads-readback-refusal.json")
		if err := writePrivate(output, "beads-readback-refusal.json", []byte("retained")); err != nil {
			t.Fatal(err)
		}
		assessBeadsReadback(output, source, submitted, &result)
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "retained" || result.Status != "BLOCK" || result.BeadsReadback != "FAIL" || result.Reason != "beads_readback_evidence_refused" || result.BeadsRefusalSHA256 != "" || partial && result.BeadsReadbackSHA256 != "" {
			t.Fatalf("unwritten evidence received credit: %+v", result)
		}
	}
}

func TestOwnedRoundtripPreservesWideNumbersThroughLiveReverseAndPrivateEvidence(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "owned-hops")
	result := run([]string{"--input", "../../internal/integration/converterroundtrip/testdata/public-complete-root-990eb800.tar", "--output-dir", output, "--engine", "memory"})
	if result.Status != "WAIT" || result.Reason != "pinned_beads_artifact_required" || result.Claims != 1 || result.Writes != 1 {
		t.Fatalf("owned restore/live-edit/reverse path refused: %+v", result)
	}
	submitted, err := os.ReadFile(filepath.Join(output, "reverse-live.jsonl"))
	if err != nil || !bytes.Contains(submitted, []byte(`"beads.timeout_ns":9007199254740993`)) {
		t.Fatal("owned live reverse rounded the original wide integer")
	}
	_, _, original, _ := originalReadbackControl(t, false)
	var expected []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(original), []byte("\n")) {
		var row map[string]any
		if err := decodeNumberJSON(line, &row); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, row)
	}
	// These adjacent-number controls cross the changed diagnostic decoder,
	// encoder and decoder, using the actual saved native target as the oracle.
	_, source, _, _ := originalReadbackControl(t, false)
	actual, err := dumpformat.ReadFile("testdata/readback-8f19/memory-complete-beads-raw-readback.tar")
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []string{"9007199254740992", "9007199254740993", "9007199254740994"} {
		var wire bytes.Buffer
		for _, row := range expected {
			if row["_type"] == "issue" {
				row["metadata"].(map[string]any)["beads.timeout_ns"] = json.Number(number)
			}
			if err := json.NewEncoder(&wire).Encode(row); err != nil {
				t.Fatal(err)
			}
		}
		cells, err := metadataReadbackCellRefusals(wire.Bytes(), actual)
		if err != nil {
			t.Fatal(err)
		}
		if number == "9007199254740992" {
			if len(cells) != 0 {
				t.Fatal("the actual equal native value was refused")
			}
			continue
		}
		if len(cells) != 1 || cells[0].Expected.Value != json.Number(number) || cells[0].Actual.Value != json.Number("9007199254740992") {
			t.Fatal("adjacent distinct integers merged in diagnostic decoder")
		}
		private := t.TempDir()
		status := outcome{Status: "BLOCK", Reason: "beads_raw_field_changed", BeadsReadback: "FAIL"}
		retainBeadsReadbackRefusal(private, source, wire.Bytes(), actual.TarBytes(), actual, nil, &status)
		refusal := readPrivateRefusal(t, private, status)
		last := refusal.CellRefusals[len(refusal.CellRefusals)-1]
		if last.Domain != "metadata" || last.Expected.Type != "number" || last.Expected.Value != json.Number(number) || last.Actual.Type != "number" || last.Actual.Value != json.Number("9007199254740992") {
			t.Fatal("private evidence encoding rounded an exact integer")
		}
	}
}
