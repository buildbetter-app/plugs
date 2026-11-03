package s1b09b12

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Version 1 files retain the historical normalized transcripts, including their
// misleading rawDigest/rawSha256 names. They do not prove raw byte equality or
// isolated credentials. Version 2 keeps exact streams and names the normalized
// digest separately. Neither version qualifies B12's unsupported apply/restore.
//
//go:embed reference/*.json record_reference.py record_reference_test.py
var referenceFiles embed.FS

const (
	referenceSourceCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	referenceSourceTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
	referenceBinarySHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
	legacyRecorderSHA256  = "3a979c3b1bb5e1e45643adf1e13ebf675d56c5549fdc9b6fa0962f5594251936"
)

type referenceCommand struct {
	Label    string   `json:"label"`
	Argv     []string `json:"argv"`
	ExitCode int      `json:"exitCode"`
	Stdout   any      `json:"stdout"`
	Stderr   any      `json:"stderr"`
}

type referenceRun struct {
	Commands     []referenceCommand `json:"commands"`
	Observations map[string]any     `json:"observations"`
	Projection   map[string]any     `json:"projection"`
	// These legacy fields describe a normalized decoded transcript, never bytes.
	LegacyNormalizedDigest string            `json:"rawDigest"`
	LegacyProjectionDigest string            `json:"projectionDigest"`
	Streams                []referenceStream `json:"streams"`
	NormalizedSHA256       string            `json:"normalizedTranscriptSha256"`
	RawStreamsSHA256       string            `json:"rawStreamsSha256"`
	ProjectionSHA256       string            `json:"projectionSha256"`
}

type referenceStream struct {
	Label        string `json:"label"`
	StdoutBase64 string `json:"stdoutBase64"`
	StderrBase64 string `json:"stderrBase64"`
	StdoutSHA256 string `json:"stdoutSha256"`
	StderrSHA256 string `json:"stderrSha256"`
}

type referenceEquality struct {
	NormalizedTranscriptsEqual bool `json:"normalizedTranscriptsEqual"`
	ProjectionsEqual           bool `json:"projectionsEqual"`
	RawStreamsEqual            bool `json:"rawStreamsEqual"`
	PerCommand                 []struct {
		Label       string `json:"label"`
		StdoutEqual bool   `json:"stdoutEqual"`
		StderrEqual bool   `json:"stderrEqual"`
	} `json:"perCommand"`
}

type referenceRecording struct {
	Schema string `json:"schema"`
	Card   string `json:"card"`
	Source struct {
		Commit           string `json:"commit"`
		Tree             string `json:"tree"`
		CleanWorkingTree bool   `json:"cleanWorkingTree"`
	} `json:"source"`
	Binary struct {
		SHA256 string `json:"sha256"`
	} `json:"binary"`
	Recorder struct {
		SHA256 string `json:"sha256"`
	} `json:"recorder"`
	First                  referenceRun   `json:"first"`
	Repeat                 referenceRun   `json:"repeat"`
	Projection             map[string]any `json:"projection"`
	ProjectionSHA256       string         `json:"projectionSha256"`
	LegacyNormalizedSHA256 string         `json:"rawSha256"`
	NormalizedSHA256       string         `json:"normalizedTranscriptSha256"`
	RawStreamsSHA256       string         `json:"rawStreamsSha256"`
	Repeated               struct {
		LegacyNormalizedSHA256 string `json:"rawSha256"`
		NormalizedSHA256       string `json:"normalizedTranscriptSha256"`
		RawStreamsSHA256       string `json:"rawStreamsSha256"`
		ProjectionSHA256       string `json:"projectionSha256"`
	} `json:"repeated"`
	Disposition struct {
		Status string `json:"status"`
		Source string `json:"source"`
	} `json:"disposition"`
	Equality      referenceEquality `json:"equality"`
	Configuration struct {
		Value struct {
			Environment struct {
				InheritAmbient *bool `json:"inheritAmbient"`
			} `json:"environment"`
		} `json:"value"`
	} `json:"configuration"`
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
		File                         string `json:"file"`
		SHA256                       string `json:"sha256"`
		LegacyNormalizedSHA256       string `json:"rawSha256"`
		LegacyRepeatNormalizedSHA256 string `json:"repeatRawSha256"`
		NormalizedSHA256             string `json:"normalizedTranscriptSha256"`
		RepeatNormalizedSHA256       string `json:"repeatNormalizedTranscriptSha256"`
		RawStreamsSHA256             string `json:"rawStreamsSha256"`
		RepeatRawStreamsSHA256       string `json:"repeatRawStreamsSha256"`
		ProjectionSHA256             string `json:"projectionSha256"`
		RepeatProjectionSHA256       string `json:"repeatProjectionSha256"`
		Status                       string `json:"status"`
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
	legacy := recording.Schema == "plug.s1-b09-b12.reference-recording.v1"
	if (!legacy && recording.Schema != "plug.s1-b09-b12.reference-recording.v2") || recording.Card != card {
		t.Fatalf("reference identity changed: %+v", recording)
	}
	if recording.Source.Commit != referenceSourceCommit || recording.Source.Tree != referenceSourceTree || recording.Binary.SHA256 != referenceBinarySHA256 {
		t.Fatalf("reference pins changed: commit=%s tree=%s binary=%s", recording.Source.Commit, recording.Source.Tree, recording.Binary.SHA256)
	}
	wantRecorder := referenceBytesDigest(recorder)
	if legacy {
		wantRecorder = legacyRecorderSHA256
	}
	if recording.Recorder.SHA256 != wantRecorder {
		t.Fatalf("recorder digest changed for %s: got %s want %s", card, recording.Recorder.SHA256, wantRecorder)
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
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatal(err)
	}
	if legacy {
		assertLegacyNormalizedDigests(t, card, recording, full)
		t.Logf("%s retains version 1 evidence only: raw bytes, clean source and credential isolation are unproven", card)
	} else {
		assertExactStreams(t, card, recording, full)
	}
	for _, command := range recording.First.Commands {
		if len(command.Argv) < 2 || command.Argv[0] != "reference-engine" {
			t.Fatalf("%s command provenance changed: %+v", card, command)
		}
	}
}

func assertLegacyNormalizedDigests(t *testing.T, card string, recording referenceRecording, full map[string]any) {
	t.Helper()
	first, repeat := recording.First, recording.Repeat
	if first.LegacyNormalizedDigest != repeat.LegacyNormalizedDigest || recording.LegacyNormalizedSHA256 != first.LegacyNormalizedDigest || recording.Repeated.LegacyNormalizedSHA256 != repeat.LegacyNormalizedDigest || first.LegacyProjectionDigest != repeat.LegacyProjectionDigest || recording.ProjectionSHA256 != first.LegacyProjectionDigest || recording.Repeated.ProjectionSHA256 != repeat.LegacyProjectionDigest {
		t.Fatalf("%s historical normalized digest summary is stale", card)
	}
	for index, run := range []referenceRun{first, repeat} {
		name := []string{"first", "repeat"}[index]
		value := full[name].(map[string]any)
		if run.LegacyNormalizedDigest != referenceDigest(map[string]any{"commands": value["commands"], "observations": value["observations"]}) || run.LegacyProjectionDigest != referenceDigest(run.Projection) {
			t.Fatalf("%s historical %s normalized/projection digest is stale", card, name)
		}
	}
}

func normalizedReferenceValue(value any, card string) any {
	switch value := value.(type) {
	case map[string]any:
		omitted := map[string]bool{}
		for _, key := range []string{"created_at", "updated_at", "closed_at", "defer_until", "heartbeat_at", "started_at", "revision", "schema_version", "owner", "lease_expires_at", "leaseExpiresAt", "createdAt", "updatedAt", "closedAt", "commit", "hash", "CommitHash", "CommitDate", "revision_hash", "revisionHash"} {
			omitted[key] = true
		}
		result := map[string]any{}
		for key, item := range value {
			if !omitted[key] {
				result[key] = normalizedReferenceValue(item, card)
			}
		}
		return result
	case []any:
		result := make([]any, len(value))
		for index, item := range value {
			result[index] = normalizedReferenceValue(item, card)
		}
		return result
	case string:
		value = regexp.MustCompile(`20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z`).ReplaceAllString(value, "<runtime-time>")
		value = regexp.MustCompile(`/tmp/plug-swarm/work/[^/\s]+`).ReplaceAllString(value, "<workspace>")
		if card == "B09" {
			value = regexp.MustCompile(`\bb09-[a-z0-9]{3,8}\b`).ReplaceAllString(value, "<generated-id>")
		}
		return value
	default:
		return value
	}
}

func decodedReferenceStream(t *testing.T, stream referenceStream) ([]byte, []byte) {
	t.Helper()
	stdout, outErr := base64.StdEncoding.Strict().DecodeString(stream.StdoutBase64)
	stderr, errErr := base64.StdEncoding.Strict().DecodeString(stream.StderrBase64)
	if outErr != nil || errErr != nil || base64.StdEncoding.EncodeToString(stdout) != stream.StdoutBase64 || base64.StdEncoding.EncodeToString(stderr) != stream.StderrBase64 || referenceBytesDigest(stdout) != stream.StdoutSHA256 || referenceBytesDigest(stderr) != stream.StderrSHA256 {
		t.Fatalf("%s exact stream encoding/digest is invalid", stream.Label)
	}
	return stdout, stderr
}

func assertExactStreams(t *testing.T, card string, recording referenceRecording, full map[string]any) {
	t.Helper()
	inherit := recording.Configuration.Value.Environment.InheritAmbient
	if !recording.Source.CleanWorkingTree || inherit == nil || *inherit || recording.Disposition.Source != "engdocs/design/plug/spec/behaviors/content.md#"+card {
		t.Fatalf("%s version 2 source/configuration/card binding is incomplete", card)
	}
	for index, run := range []referenceRun{recording.First, recording.Repeat} {
		name := []string{"first", "repeat"}[index]
		value := full[name].(map[string]any)
		if run.NormalizedSHA256 != referenceDigest(map[string]any{"commands": value["commands"], "observations": value["observations"]}) || run.RawStreamsSHA256 != referenceDigest(value["streams"]) || run.ProjectionSHA256 != referenceDigest(run.Projection) {
			t.Fatalf("%s %s stream/normalized/projection digest is stale", card, name)
		}
		if len(run.Streams) != len(run.Commands) {
			t.Fatalf("%s %s exact streams are unpaired", card, name)
		}
		for i, stream := range run.Streams {
			command := run.Commands[i]
			stdout, stderr := decodedReferenceStream(t, stream)
			var decoded any
			if err := json.Unmarshal(stdout, &decoded); err != nil {
				decoded = string([]rune(string(stdout)))
			}
			if stream.Label != command.Label || !reflect.DeepEqual(normalizedReferenceValue(decoded, card), command.Stdout) || !reflect.DeepEqual(normalizedReferenceValue(string([]rune(string(stderr))), card), command.Stderr) {
				t.Fatalf("%s %s exact streams do not produce their normalized command", card, stream.Label)
			}
		}
	}
	first, repeat := recording.First, recording.Repeat
	if first.NormalizedSHA256 != repeat.NormalizedSHA256 || first.ProjectionSHA256 != repeat.ProjectionSHA256 || recording.NormalizedSHA256 != first.NormalizedSHA256 || recording.RawStreamsSHA256 != first.RawStreamsSHA256 || recording.ProjectionSHA256 != first.ProjectionSHA256 || recording.Repeated.NormalizedSHA256 != repeat.NormalizedSHA256 || recording.Repeated.RawStreamsSHA256 != repeat.RawStreamsSHA256 || recording.Repeated.ProjectionSHA256 != repeat.ProjectionSHA256 {
		t.Fatalf("%s version 2 digest summary is stale", card)
	}
	if !recording.Equality.NormalizedTranscriptsEqual || !recording.Equality.ProjectionsEqual || len(recording.Equality.PerCommand) != len(first.Streams) {
		t.Fatalf("%s equality summary is incomplete", card)
	}
	rawEqual := true
	for index, left := range first.Streams {
		leftOut, leftErr := decodedReferenceStream(t, left)
		rightOut, rightErr := decodedReferenceStream(t, repeat.Streams[index])
		equality := recording.Equality.PerCommand[index]
		outEqual, errEqual := bytes.Equal(leftOut, rightOut), bytes.Equal(leftErr, rightErr)
		if equality.Label != left.Label || equality.StdoutEqual != outEqual || equality.StderrEqual != errEqual {
			t.Fatalf("%s %s raw byte equality summary is stale", card, left.Label)
		}
		rawEqual = rawEqual && outEqual && errEqual
	}
	if recording.Equality.RawStreamsEqual != rawEqual {
		t.Fatalf("%s raw byte equality claim disagrees with exact streams", card)
	}
}

func TestS1ReferenceRecorderContract(t *testing.T) {
	// These are recorder-policy unit tests, not Beads or paired-store evidence.
	directory := t.TempDir()
	for _, name := range []string{"record_reference.py", "record_reference_test.py"} {
		data, err := referenceFiles.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("python3", "-B", filepath.Join(directory, "record_reference_test.py"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("reference recorder policy tests failed: %v\n%s", err, output)
	}
	t.Log(string(output))
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
	legacy := provenance.Schema == "plug.s1-b09-b12.reference-provenance.v1"
	wantRecorder := referenceBytesDigest(recorder)
	if legacy {
		wantRecorder = legacyRecorderSHA256
	}
	if (!legacy && provenance.Schema != "plug.s1-b09-b12.reference-provenance.v2") || provenance.Source.Commit != referenceSourceCommit || provenance.Source.Tree != referenceSourceTree || provenance.Binary.SHA256 != referenceBinarySHA256 || provenance.Recorder.SHA256 != wantRecorder {
		t.Fatalf("reference provenance changed: %+v", provenance)
	}
	if provenance.Recording.Runs != 2 || !provenance.Recording.IndependentFreshGitWorkspaces || !provenance.Recording.NormalizedTranscriptsEqual || !provenance.Recording.ProjectionsEqual {
		t.Fatalf("paired recording controls are incomplete: %+v", provenance.Recording)
	}

	for _, card := range []string{"B09", "B10", "B11", "B12"} {
		recording, raw := readRecording(t, card)
		if legacy != (recording.Schema == "plug.s1-b09-b12.reference-recording.v1") {
			t.Fatalf("%s recording/provenance versions differ", card)
		}
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
		if entry.ProjectionSHA256 != recording.ProjectionSHA256 || entry.RepeatProjectionSHA256 != recording.Repeated.ProjectionSHA256 || entry.Status != recording.Disposition.Status {
			t.Fatalf("provenance summary for %s is stale: %+v", card, entry)
		}
		if legacy {
			if entry.LegacyNormalizedSHA256 != recording.LegacyNormalizedSHA256 || entry.LegacyRepeatNormalizedSHA256 != recording.Repeated.LegacyNormalizedSHA256 {
				t.Fatalf("historical normalized digest summary for %s is stale", card)
			}
		} else if entry.NormalizedSHA256 != recording.NormalizedSHA256 || entry.RepeatNormalizedSHA256 != recording.Repeated.NormalizedSHA256 || entry.RawStreamsSHA256 != recording.RawStreamsSHA256 || entry.RepeatRawStreamsSHA256 != recording.Repeated.RawStreamsSHA256 {
			t.Fatalf("stream/normalized digest summary for %s is stale", card)
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

	t.Log("B09-B12 paired normalized decoded transcripts and semantic projections match; version 1 has no raw byte proof, and B12 apply/restore remains unsupported and unqualified")
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
		digests := map[string]string{"projection": recording.ProjectionSHA256, "file": referenceBytesDigest(raw)}
		if recording.Schema == "plug.s1-b09-b12.reference-recording.v1" {
			digests["legacy normalized"] = recording.LegacyNormalizedSHA256
		} else {
			digests["normalized"] = recording.NormalizedSHA256
			digests["exact stream envelope"] = recording.RawStreamsSHA256
		}
		for name, value := range digests {
			if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(value) {
				t.Fatalf("%s %s digest has invalid shape: %q", card, name, value)
			}
		}
	}
}
