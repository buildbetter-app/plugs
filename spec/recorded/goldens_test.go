package recorded

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The file is the reviewed reference artifact. This test intentionally has no
// Plug runtime dependency: it proves that expected values came from the
// separately recorded reference runner and that their bytes have not drifted.
//
//go:embed goldens.json
var goldenFile []byte

const pinnedCommit = "9d6fff760b7a314b476539198504d77ce0959656"
const pinnedBinarySHA256 = "e7a70432e3137161c8d8a7530e1db4236bb5ac8a36d7de41f5b831fdbde2f776"

var expectedRecordings = map[string]struct {
	digest      string
	artifactSHA string
	sourceSHA   string
	sourcePath  string
	sourceCount int
}{
	"B17": {
		digest:      "aab4a8ef882b6486503960589fefd36e5749daa7745539f5bf3fd71aa9ebd689",
		artifactSHA: "3ed940892a2cc2fdadb0a9ad789868eb50ad50924f81975985aef58cd526b75b",
		sourceSHA:   "999217f9457e9edd28d992206284051c5d110fdced9b36675160a4ad9845314a",
		sourcePath:  "internal/storage/embeddeddolt/build09_record_test.go",
		sourceCount: 11,
	},
	"B24": {
		digest:      "9fddd7a374875eeff92502f26114386b34c78e121f970a506cf32d54e9bea1b4",
		artifactSHA: "70ac84166752614db8d449690b6c7f093bc860f2b50682d4151b2ccd155024f1",
		sourceSHA:   "4ad3e54d67eefe5b610099257f1a662ed6b57628eb403e16eff13c4311d5ce31",
		sourcePath:  "build09_record_b24.py",
		sourceCount: 2,
	},
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type manifest struct {
	Format     string                  `json:"format"`
	Provenance provenance              `json:"provenance"`
	Recorders  map[string]recordedCard `json:"recorders"`
}

type provenance struct {
	SourceRepository string   `json:"sourceRepository"`
	SourceRef        string   `json:"sourceRef"`
	SourceCommit     string   `json:"sourceCommit"`
	Binary           binary   `json:"binary"`
	Runtime          string   `json:"runtime"`
	RecordedCards    []string `json:"recordedCards"`
	ReferenceEngine  string   `json:"referenceEngine"`
	ClockNote        string   `json:"clockNote"`
}

type binary struct {
	BuildCommand string `json:"buildCommand"`
	Path         string `json:"path"`
	Version      string `json:"version"`
	SHA256       string `json:"sha256"`
}

type recordedCard struct {
	SourceTests          []string     `json:"sourceTests"`
	SourceFiles          []sourceFile `json:"sourceFiles"`
	Commands             []string     `json:"commands"`
	RawJSON              string       `json:"rawJSON"`
	Digest               string       `json:"digest"`
	ArtifactSHA256       string       `json:"artifactSha256"`
	RepeatArtifactSHA256 string       `json:"repeatArtifactSha256"`
}

type sourceFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func decodeManifest(data []byte) (manifest, error) {
	var got manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&got); err != nil {
		return manifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return manifest{}, fmt.Errorf("trailing JSON value")
		}
		return manifest{}, err
	}
	return got, nil
}

func readManifest(t *testing.T) (manifest, []byte) {
	t.Helper()
	got, err := decodeManifest(goldenFile)
	if err != nil {
		t.Fatalf("decode recorded golden manifest: %v", err)
	}
	return got, goldenFile
}

func requireDigest(t *testing.T, label, value string) {
	t.Helper()
	if !sha256Pattern.MatchString(value) {
		t.Fatalf("%s is not a lowercase SHA-256 digest: %q", label, value)
	}
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestRecordedGoldensHavePinnedProvenance(t *testing.T) {
	got, _ := readManifest(t)
	if got.Format != "plug-recorded-goldens/1" {
		t.Fatalf("format = %q", got.Format)
	}
	if got.Provenance.SourceCommit != pinnedCommit || got.Provenance.SourceRef != "refs/tags/beads-base" {
		t.Fatalf("source pin = %q %q", got.Provenance.SourceRef, got.Provenance.SourceCommit)
	}
	if got.Provenance.SourceRepository == "" || got.Provenance.Runtime == "" || got.Provenance.ReferenceEngine != "embedded-dolt" || got.Provenance.ClockNote == "" {
		t.Fatalf("incomplete source provenance: %+v", got.Provenance)
	}
	if got.Provenance.Binary.SHA256 != pinnedBinarySHA256 || got.Provenance.Binary.Version == "" || got.Provenance.Binary.Path == "" || got.Provenance.Binary.BuildCommand == "" {
		t.Fatalf("incomplete binary provenance: %+v", got.Provenance.Binary)
	}
	requireDigest(t, "binary SHA-256", got.Provenance.Binary.SHA256)
	cards := append([]string(nil), got.Provenance.RecordedCards...)
	sort.Strings(cards)
	if !equalStrings(cards, []string{"B17", "B24"}) {
		t.Fatalf("recorded cards = %v", cards)
	}
	if len(got.Recorders) != 2 {
		t.Fatalf("recorder count = %d", len(got.Recorders))
	}
}

func TestRecordedVectorsReplayTheirIndependentDigests(t *testing.T) {
	got, _ := readManifest(t)
	for _, card := range []string{"B17", "B24"} {
		recorder, ok := got.Recorders[card]
		if !ok {
			t.Fatalf("missing recorder %s", card)
		}
		t.Run(card, func(t *testing.T) {
			want := expectedRecordings[card]
			if len(recorder.SourceTests) != want.sourceCount || len(recorder.SourceFiles) != 1 || len(recorder.Commands) != 2 {
				t.Fatalf("incomplete recorder provenance: %+v", recorder)
			}
			for i, command := range recorder.Commands {
				if strings.TrimSpace(command) == "" || !strings.Contains(command, "build09-") {
					t.Fatalf("command %d does not identify the pinned recorder checkout: %q", i, command)
				}
			}
			for _, file := range recorder.SourceFiles {
				if file.Path == "" {
					t.Fatal("recorder source path is blank")
				}
				requireDigest(t, file.Path, file.SHA256)
				if file.Path != want.sourcePath || file.SHA256 != want.sourceSHA {
					t.Fatalf("recorder source = %s %s, want pinned %s %s", file.Path, file.SHA256, want.sourcePath, want.sourceSHA)
				}
			}
			if recorder.Digest != want.digest {
				t.Fatalf("vector digest = %s, want pinned %s", recorder.Digest, want.digest)
			}
			if recorder.ArtifactSHA256 != want.artifactSHA || recorder.RepeatArtifactSHA256 != want.artifactSHA {
				t.Fatalf("recording artifact hash drifted: %s %s", recorder.ArtifactSHA256, recorder.RepeatArtifactSHA256)
			}
			requireDigest(t, "vector digest", recorder.Digest)
			requireDigest(t, "first artifact digest", recorder.ArtifactSHA256)
			requireDigest(t, "repeat artifact digest", recorder.RepeatArtifactSHA256)
			if recorder.ArtifactSHA256 != recorder.RepeatArtifactSHA256 {
				t.Fatal("independent recording artifacts differ")
			}
			if digest([]byte(recorder.RawJSON)) != recorder.Digest {
				t.Fatalf("raw vector digest = %s, want %s", digest([]byte(recorder.RawJSON)), recorder.Digest)
			}
			var vector map[string]any
			if err := json.Unmarshal([]byte(recorder.RawJSON), &vector); err != nil {
				t.Fatalf("raw vector is invalid JSON: %v", err)
			}
			if vector["card"] != card || vector["engine"] != "embedded-dolt" {
				t.Fatalf("vector identity = %#v", vector)
			}
			canonical, err := json.Marshal(vector)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(canonical, []byte(recorder.RawJSON)) {
				t.Fatalf("raw vector is not the recorder's canonical JSON bytes")
			}
			if strings.Contains(strings.ToLower(recorder.RawJSON), "plug") {
				t.Fatal("reference vector contains Plug-derived expected data")
			}
		})
	}
}

func TestRecordedManifestRejectsTrailingData(t *testing.T) {
	data := append(append([]byte(nil), goldenFile...), []byte("{}")...)
	if _, err := decodeManifest(data); err == nil {
		t.Fatal("decoder accepted a second manifest value")
	}
}

func TestRecordedManifestIsEmbedded(t *testing.T) {
	if len(goldenFile) == 0 {
		t.Fatal("golden manifest was not embedded")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
