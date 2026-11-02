package s1b09b12

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The four files are complete normalized command transcripts from two
// independent fresh Beads workspaces. The test derives the retained semantic
// projection from the embedded JSON and keeps the older synthetic vectors in
// replay_test.go as a separate Plug-only fixture.
//
//go:embed reference/*.json record_reference.py
var referenceFiles embed.FS

const (
	referenceSourceCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	referenceSourceTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
	referenceBinarySHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
)

type referenceCommand struct {
	Label    string   `json:"label"`
	Argv     []string `json:"argv"`
	ExitCode int      `json:"exitCode"`
	Stdout   any      `json:"stdout"`
	Stderr   any      `json:"stderr"`
}

type referenceRun struct {
	Commands         []referenceCommand `json:"commands"`
	Observations     map[string]any     `json:"observations"`
	Projection       map[string]any     `json:"projection"`
	RawSHA256        string             `json:"rawSha256"`
	ProjectionSHA256 string             `json:"projectionDigest"`
}

type referenceRecording struct {
	Schema       string `json:"schema"`
	Card         string `json:"card"`
	SourceCommit string `json:"sourceCommit"`
	SourceTree   string `json:"sourceTree"`
	BinarySHA256 string `json:"binarySha256"`
	Recorder     struct {
		SHA256 string `json:"sha256"`
	} `json:"recorder"`
	First            referenceRun   `json:"first"`
	Repeat           referenceRun   `json:"repeat"`
	Projection       map[string]any `json:"projection"`
	ProjectionSHA256 string         `json:"projectionSha256"`
	RawSHA256        string         `json:"rawSha256"`
	Repeated         struct {
		RawSHA256        string `json:"rawSha256"`
		ProjectionSHA256 string `json:"projectionSha256"`
	} `json:"repeated"`
	Disposition struct {
		Status string `json:"status"`
	} `json:"disposition"`
}

type referenceProvenance struct {
	Schema string `json:"schema"`
	Source struct {
		Commit string `json:"commit"`
		Tree   string `json:"tree"`
	} `json:"source"`
	Binary struct {
		SHA256 string `json:"sha256"`
	} `json:"binary"`
	Recorder struct {
		File   string `json:"file"`
		SHA256 string `json:"sha256"`
	} `json:"recorder"`
	Recording struct {
		Runs                          int  `json:"runs"`
		IndependentFreshGitWorkspaces bool `json:"independentFreshGitWorkspaces"`
		NormalizedTranscriptsEqual    bool `json:"normalizedTranscriptsEqual"`
		ProjectionsEqual              bool `json:"projectionsEqual"`
	} `json:"recording"`
	Cards map[string]struct {
		File                   string `json:"file"`
		SHA256                 string `json:"sha256"`
		RawSHA256              string `json:"rawSha256"`
		RepeatRawSHA256        string `json:"repeatRawSha256"`
		ProjectionSHA256       string `json:"projectionSha256"`
		RepeatProjectionSHA256 string `json:"repeatProjectionSha256"`
		Status                 string `json:"status"`
	} `json:"cards"`
}

func referenceDigest(value any) string {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		panic(err)
	}
	data := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func referenceBytesDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func readRecording(t *testing.T, card string) (referenceRecording, []byte) {
	t.Helper()
	data, err := referenceFiles.ReadFile("reference/" + cardToFile(card))
	if err != nil {
		t.Fatal(err)
	}
	var recording referenceRecording
	if err := json.Unmarshal(data, &recording); err != nil {
		t.Fatalf("decode %s recording: %v", card, err)
	}
	return recording, data
}

func commandMap(commands []referenceCommand) map[string]referenceCommand {
	result := make(map[string]referenceCommand, len(commands))
	for _, command := range commands {
		if _, exists := result[command.Label]; exists {
			panic("duplicate reference command label: " + command.Label)
		}
		result[command.Label] = command
	}
	return result
}

func assertRecording(t *testing.T, card string, recording referenceRecording, raw []byte, recorder []byte) {
	t.Helper()
	if recording.Schema != "plug.s1-b09-b12.reference-recording.v1" || recording.Card != card {
		t.Fatalf("reference identity changed: %+v", recording)
	}
	if recording.SourceCommit != referenceSourceCommit || recording.SourceTree != referenceSourceTree || recording.BinarySHA256 != referenceBinarySHA256 {
		t.Fatalf("reference pins changed: commit=%s tree=%s binary=%s", recording.SourceCommit, recording.SourceTree, recording.BinarySHA256)
	}
	if recording.Recorder.SHA256 != referenceBytesDigest(recorder) {
		t.Fatalf("recorder digest changed for %s: got %s want %s", card, recording.Recorder.SHA256, referenceBytesDigest(recorder))
	}
	if len(recording.First.Commands) == 0 || len(recording.First.Commands) != len(recording.Repeat.Commands) {
		t.Fatalf("%s command transcripts are empty or unpaired", card)
	}
	if !reflect.DeepEqual(recording.First.Commands, recording.Repeat.Commands) {
		t.Fatalf("%s normalized independent command transcripts differ", card)
	}
	if !reflect.DeepEqual(recording.First.Projection, recording.Repeat.Projection) || !reflect.DeepEqual(recording.First.Projection, recording.Projection) {
		t.Fatalf("%s independent semantic projections differ", card)
	}
	if recording.First.RawSHA256 != recording.Repeat.RawSHA256 || recording.First.ProjectionSHA256 != recording.Repeat.ProjectionSHA256 || recording.RawSHA256 != recording.First.RawSHA256 || recording.ProjectionSHA256 != recording.First.ProjectionSHA256 {
		t.Fatalf("%s paired digests differ or do not match top-level", card)
	}
	if recording.Repeated.RawSHA256 != recording.Repeat.RawSHA256 || recording.Repeated.ProjectionSHA256 != recording.Repeat.ProjectionSHA256 {
		t.Fatalf("%s repeated digest summary is stale", card)
	}
	if got := referenceDigest(recording.First.Projection); got != recording.First.ProjectionSHA256 {
		t.Fatalf("%s projection digest=%s recomputed=%s", card, recording.First.ProjectionSHA256, got)
	}
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatal(err)
	}
	if got := referenceDigest(map[string]any{"commands": full["first"].(map[string]any)["commands"], "observations": full["first"].(map[string]any)["observations"]}); got != recording.First.RawSHA256 {
		t.Fatalf("%s raw digest=%s recomputed=%s", card, recording.First.RawSHA256, got)
	}
	for _, command := range recording.First.Commands {
		if len(command.Argv) < 2 || command.Argv[0] != "reference-engine" {
			t.Fatalf("%s command provenance changed: %+v", card, command)
		}
	}
}

func assertAllSuccessfulExcept(t *testing.T, commands []referenceCommand, failed ...string) {
	t.Helper()
	failures := map[string]bool{}
	for _, label := range failed {
		failures[label] = true
	}
	for _, command := range commands {
		if failures[command.Label] {
			if command.ExitCode == 0 {
				t.Fatalf("%s refusal %s unexpectedly succeeded", command.Label, command.Label)
			}
			continue
		}
		if command.ExitCode != 0 {
			t.Fatalf("unexpected %s command failure: %+v", command.Label, command)
		}
	}
}

func boolProjection(t *testing.T, card string, projection map[string]any, key string) {
	t.Helper()
	value, ok := projection[key].(bool)
	if !ok || !value {
		t.Fatalf("%s projection %s=%v, want true", card, key, projection[key])
	}
}

func TestS1PinnedBeadsReferenceRecordingsAndIndependentProjections(t *testing.T) {
	recorder, err := referenceFiles.ReadFile("record_reference.py")
	if err != nil {
		t.Fatal(err)
	}
	provenanceRaw, err := referenceFiles.ReadFile("reference/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var provenance referenceProvenance
	if err := json.Unmarshal(provenanceRaw, &provenance); err != nil {
		t.Fatal(err)
	}
	if provenance.Schema != "plug.s1-b09-b12.reference-provenance.v1" || provenance.Source.Commit != referenceSourceCommit || provenance.Source.Tree != referenceSourceTree || provenance.Binary.SHA256 != referenceBinarySHA256 || provenance.Recorder.SHA256 != referenceBytesDigest(recorder) {
		t.Fatalf("reference provenance changed: %+v", provenance)
	}
	if provenance.Recording.Runs != 2 || !provenance.Recording.IndependentFreshGitWorkspaces || !provenance.Recording.NormalizedTranscriptsEqual || !provenance.Recording.ProjectionsEqual {
		t.Fatalf("paired recording controls are incomplete: %+v", provenance.Recording)
	}

	for _, card := range []string{"B09", "B10", "B11", "B12"} {
		recording, raw := readRecording(t, card)
		assertRecording(t, card, recording, raw, recorder)
		entry, ok := provenance.Cards[card]
		if !ok {
			t.Fatalf("provenance has no %s card entry", card)
		}
		if entry.File != cardToFile(card) {
			t.Fatalf("provenance file for %s=%q", card, entry.File)
		}
		fileBytes, err := referenceFiles.ReadFile("reference/" + entry.File)
		if err != nil {
			t.Fatal(err)
		}
		if referenceBytesDigest(fileBytes) != entry.SHA256 {
			t.Fatalf("provenance file digest for %s is stale", card)
		}
		if entry.RawSHA256 != recording.RawSHA256 || entry.RepeatRawSHA256 != recording.Repeated.RawSHA256 || entry.ProjectionSHA256 != recording.ProjectionSHA256 || entry.RepeatProjectionSHA256 != recording.Repeated.ProjectionSHA256 || entry.Status != recording.Disposition.Status {
			t.Fatalf("provenance summary for %s is stale: %+v", card, entry)
		}
	}

	b09, _ := readRecording(t, "B09")
	for _, key := range []string{"createdIDsDistinct", "createdIDPattern", "sameSemanticFields", "collisionRefused"} {
		boolProjection(t, "B09", b09.Projection, key)
	}
	if b09.Projection["rowsAfterCollision"] != float64(2) {
		t.Fatalf("B09 rowsAfterCollision=%v", b09.Projection["rowsAfterCollision"])
	}
	assertAllSuccessfulExcept(t, b09.First.Commands, "hash-collision-refusal")

	b10, _ := readRecording(t, "B10")
	for _, key := range []string{"explicitHighWater", "mintedAfterImported", "sequentialContinuation", "noReuseAfterDeleteAndClose", "childRowsAfterCleanup"} {
		boolProjection(t, "B10", b10.Projection, key)
	}
	wantIDs := []any{"b10-parent.8", "b10-parent.9", "b10-parent.10", "b10-parent.11", "b10-parent.12", "b10-parent.13"}
	if !reflect.DeepEqual(b10.Projection["observedIDs"], wantIDs) {
		t.Fatalf("B10 observedIDs=%v want=%v", b10.Projection["observedIDs"], wantIDs)
	}
	assertAllSuccessfulExcept(t, b10.First.Commands)

	b11, _ := readRecording(t, "B11")
	for _, key := range []string{"sameTypeReaddAccepted", "typedRetypeRefused", "retypeNoWrite", "unlinkThenDifferentTypeAccepted", "unlinkConservesEndpoints"} {
		boolProjection(t, "B11", b11.Projection, key)
	}
	assertAllSuccessfulExcept(t, b11.First.Commands, "retype-refusal")

	b12, _ := readRecording(t, "B12")
	for _, key := range []string{"analyzeSucceeded", "applyRefusedInEmbeddedMode", "restoreReadRefusedWithoutSnapshot", "restoreApplyRefusedWithoutSnapshot"} {
		boolProjection(t, "B12", b12.Projection, key)
	}
	if b12.Projection["analyzeCandidateCount"] != float64(0) || b12.Disposition.Status != "deviate" {
		t.Fatalf("B12 source disposition changed: projection=%v disposition=%s", b12.Projection, b12.Disposition.Status)
	}
	assertAllSuccessfulExcept(t, b12.First.Commands, "admin-compact-apply", "restore-read", "restore-apply")

	t.Log("B09-B12 pinned Beads recordings PASS: paired normalized transcripts and semantic projections are byte-stable; B12's embedded-mode refusal remains an explicit deviation")
}

func cardToFile(card string) string {
	return strings.ToLower(card) + ".json"
}

func TestS1ReferenceProjectionKeysRemainBounded(t *testing.T) {
	allowed := map[string][]string{
		"B09": {"collisionRefused", "createdIDPattern", "createdIDsDistinct", "rowsAfterCollision", "sameSemanticFields"},
		"B10": {"childRowsAfterCleanup", "explicitHighWater", "mintedAfterImported", "noReuseAfterDeleteAndClose", "observedIDs", "sequentialContinuation"},
		"B11": {"retypeNoWrite", "sameTypeReaddAccepted", "typedRetypeRefused", "unlinkConservesEndpoints", "unlinkThenDifferentTypeAccepted"},
		"B12": {"analyzeCandidateCount", "analyzeSucceeded", "applyRefusedInEmbeddedMode", "restoreApplyRefusedWithoutSnapshot", "restoreReadRefusedWithoutSnapshot"},
	}
	for card, keys := range allowed {
		recording, _ := readRecording(t, card)
		got := make([]string, 0, len(recording.Projection))
		for key := range recording.Projection {
			got = append(got, key)
		}
		sort.Strings(got)
		sort.Strings(keys)
		if !reflect.DeepEqual(got, keys) {
			t.Fatalf("%s projection keys=%v want=%v", card, got, keys)
		}
	}
}

func TestS1ReferenceArtifactDigestShape(t *testing.T) {
	for _, card := range []string{"B09", "B10", "B11", "B12"} {
		recording, raw := readRecording(t, card)
		for name, value := range map[string]string{"raw": recording.RawSHA256, "projection": recording.ProjectionSHA256, "file": referenceBytesDigest(raw)} {
			if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(value) {
				t.Fatalf("%s %s digest has invalid shape: %q", card, name, value)
			}
		}
	}
}
