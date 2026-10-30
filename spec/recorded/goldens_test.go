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

//go:embed provenance.py
var provenanceSource []byte

const pinnedCommit = "9d6fff760b7a314b476539198504d77ce0959656"
const pinnedBinarySHA256 = "e7a70432e3137161c8d8a7530e1db4236bb5ac8a36d7de41f5b831fdbde2f776"
const pinnedDriverVersion = "github.com/dolthub/dolt/go@v0.40.5-0.20260806213044-796d07497741"
const pinnedGoldenSHA256 = "26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32"

var expectedRecordings = map[string]struct {
	digest              string
	artifactSHA         string
	sourceSHA           string
	sourcePath          string
	sourceCount         int
	sourceEvidenceSHA   string
	projectionDigest    string
	configurationDigest string
	inputDigest         string
	provenanceDigest    string
	projectionPaths     []string
}{
	"B17": {
		digest:              "e91e136dd7fd0455a8c7416d9fd37ac252574ec9b7752934233b098e406d653b",
		artifactSHA:         "fff0ec437df90cef03a0937786ea4e2326d28ffd8171fcbf56131ae69644480e",
		sourceSHA:           "82041707d89878b8dd48cc7478af368038672605432394024cdb8658dbc875aa",
		sourcePath:          "internal/storage/embeddeddolt/build09_record_test.go",
		sourceCount:         13,
		sourceEvidenceSHA:   "31f38457d6d1572294ca7d7183d656bcf1d1b646ad348df26440ede0cb72c6eb",
		projectionDigest:    "4ad0342c3d500df6e0fa41a95e377af04a30aee66a4f7aa17d9f0e161bcfc3bd",
		configurationDigest: "ab7f7941c4ff04ab93c76c331aa77612df1b3559243699e9a7cdc1b0056f45f3",
		inputDigest:         "cf7583f5f30291e29f36433a2e4dfab3958f7d48bb9ece90436b3d98b6182ea1",
		provenanceDigest:    "b8e81989474ea64f82a8ecc2aebf6fe43b50308f06bd5e8ff5697117afd5c3ce",
		projectionPaths:     []string{"card", "engine", "outcomes", "observations"},
	},
	"B24": {
		digest:              "9fddd7a374875eeff92502f26114386b34c78e121f970a506cf32d54e9bea1b4",
		artifactSHA:         "70ac84166752614db8d449690b6c7f093bc860f2b50682d4151b2ccd155024f1",
		sourceSHA:           "4ad3e54d67eefe5b610099257f1a662ed6b57628eb403e16eff13c4311d5ce31",
		sourcePath:          "build09_record_b24.py",
		sourceCount:         2,
		sourceEvidenceSHA:   "45825590fe8e58241eecf6ca77ba56d74881dd8a77018c3830309cd37de4a525",
		projectionDigest:    "ccb04e27ec44c9c7a32b4bbb8be959f85d14dcebf272d0b5c95bf5310ca3a956",
		configurationDigest: "aa3f41cebf0780d64902811ef701013419aeee2112ff789d43879b9e7c45351a",
		inputDigest:         "5ee49df6ad5c7442241cd716824b3a5fd311ed238a3d670011a69f7fa2720abd",
		provenanceDigest:    "8e42d2d01b71630f4690c87c6f6ecd247c399bbd0f406f111b87a034f934506c",
		projectionPaths:     []string{"card", "engine", "inputs", "outcomes"},
	},
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type manifest struct {
	Format     string                  `json:"format"`
	Provenance provenance              `json:"provenance"`
	Recorders  map[string]recordedCard `json:"recorders"`
}

type provenance struct {
	SourceRepository string           `json:"sourceRepository"`
	SourceRef        string           `json:"sourceRef"`
	SourceCommit     string           `json:"sourceCommit"`
	Binary           binary           `json:"binary"`
	Runtime          string           `json:"runtime"`
	RecordedCards    []string         `json:"recordedCards"`
	ReferenceEngine  string           `json:"referenceEngine"`
	ClockNote        string           `json:"clockNote"`
	Recorder         recorderIdentity `json:"recorder"`
	Driver           driverIdentity   `json:"driver"`
	Schema           schemaCursors    `json:"schema"`
	Configuration    digestEvidence   `json:"configuration"`
}

type recorderIdentity struct {
	Identity string `json:"identity"`
	Version  string `json:"version"`
	Source   string `json:"source"`
	SHA256   string `json:"sha256"`
}

type driverIdentity struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type schemaCursors struct {
	MainCursor    int    `json:"mainCursor"`
	IgnoredCursor int    `json:"ignoredCursor"`
	MainDigest    string `json:"mainDigest"`
	IgnoredDigest string `json:"ignoredDigest"`
	CursorSource  string `json:"cursorSource"`
}

type binary struct {
	BuildCommand string `json:"buildCommand"`
	Path         string `json:"path"`
	Version      string `json:"version"`
	SHA256       string `json:"sha256"`
}

type recordedCard struct {
	SourceTests          []string                   `json:"sourceTests"`
	SourceFiles          []sourceFile               `json:"sourceFiles"`
	Commands             []string                   `json:"commands"`
	RawJSON              string                     `json:"rawJSON"`
	Digest               string                     `json:"digest"`
	ArtifactSHA256       string                     `json:"artifactSha256"`
	RepeatArtifactSHA256 string                     `json:"repeatArtifactSha256"`
	Disposition          dispositionEvidence        `json:"disposition"`
	Configuration        digestEvidence             `json:"configuration"`
	Inputs               digestEvidence             `json:"inputs"`
	Projection           projectionEvidence         `json:"projection"`
	RawDigest            string                     `json:"rawDigest"`
	ProjectionDigest     string                     `json:"projectionDigest"`
	RepeatedProvenance   repeatedProvenanceEvidence `json:"repeatedProvenance"`
}

type dispositionEvidence struct {
	Status string `json:"status"`
	Source string `json:"source"`
	Scope  string `json:"scope"`
}

type digestEvidence struct {
	Value  json.RawMessage `json:"value"`
	Digest string          `json:"digest"`
}

type projectionEvidence struct {
	Format string   `json:"format"`
	Paths  []string `json:"paths"`
	JSON   string   `json:"json"`
	Digest string   `json:"digest"`
}

type repeatedProvenanceEvidence struct {
	First        string `json:"first"`
	Repeat       string `json:"repeat"`
	FirstDigest  string `json:"firstDigest"`
	RepeatDigest string `json:"repeatDigest"`
}

type sourceFile struct {
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	Content string `json:"content"`
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

func decodeGoldenManifest(t *testing.T) manifest {
	t.Helper()
	got, err := decodeManifest(goldenFile)
	if err != nil {
		t.Fatalf("decode recorded golden manifest: %v", err)
	}
	return got
}

func readManifest(t *testing.T) (manifest, []byte) {
	t.Helper()
	if digest(goldenFile) != pinnedGoldenSHA256 {
		t.Fatalf("recorded golden bytes drifted: %s", digest(goldenFile))
	}
	got := decodeGoldenManifest(t)
	if err := validateManifest(got); err != nil {
		t.Fatalf("validate recorded golden manifest: %v", err)
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

func sourceEvidenceDigest(sourceTests, commands []string) string {
	value := struct {
		SourceTests []string `json:"sourceTests"`
		Commands    []string `json:"commands"`
	}{sourceTests, commands}
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return digest(raw)
}

func canonicalJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, err
	}
	return json.Marshal(value)
}

func validateDigestEvidence(label string, evidence digestEvidence) error {
	if len(evidence.Value) == 0 {
		return fmt.Errorf("%s value is missing", label)
	}
	canonical, err := canonicalJSON(evidence.Value)
	if err != nil {
		return fmt.Errorf("%s value: %w", label, err)
	}
	if len(canonical) == 0 || canonical[0] != '{' {
		return fmt.Errorf("%s value must be an object", label)
	}
	if !sha256Pattern.MatchString(evidence.Digest) {
		return fmt.Errorf("%s digest is missing or malformed", label)
	}
	if digest(canonical) != evidence.Digest {
		return fmt.Errorf("%s digest does not match canonical value", label)
	}
	return nil
}

func validateManifest(got manifest) error {
	if got.Format != "plug-recorded-goldens/1" {
		return fmt.Errorf("format = %q", got.Format)
	}
	p := got.Provenance
	if p.SourceRepository != "beads-base provenance tag in buildbetter-app/plugs" || p.SourceRef != "refs/tags/beads-base" || p.SourceCommit != pinnedCommit || p.Runtime == "" || p.ReferenceEngine != "embedded-dolt" || p.ClockNote == "" {
		return fmt.Errorf("incomplete source provenance")
	}
	if p.Binary.SHA256 != pinnedBinarySHA256 || p.Binary.Version == "" || p.Binary.Path == "" || p.Binary.BuildCommand == "" {
		return fmt.Errorf("incomplete executable provenance")
	}
	if p.Recorder.Identity != "build09-reference-provenance" || p.Recorder.Version != "2" || p.Recorder.Source != "spec/recorded/provenance.py" || p.Recorder.SHA256 != digest(provenanceSource) {
		return fmt.Errorf("incomplete or mismatched recorder identity/version/source")
	}
	if p.Driver.Name != "embedded-dolt" || p.Driver.Version != pinnedDriverVersion {
		return fmt.Errorf("incomplete or mismatched driver identity/version")
	}
	wantSchema := schemaCursors{
		MainCursor: 72, IgnoredCursor: 28,
		MainDigest:    "c649854f6d67016df0db1c3b324679a41e090b8fad0ae5ac5d94cf72d5e8f0f2",
		IgnoredDigest: "31abcfd3374b7e7ae7b95727785a1591107cac63cbf5db59ea6685350c05b470",
		CursorSource:  "pinned migration inventory; B17 also queries initialized schema tables",
	}
	if p.Schema != wantSchema {
		return fmt.Errorf("incomplete or mismatched paired schema evidence")
	}
	if err := validateDigestEvidence("provenance.configuration", p.Configuration); err != nil {
		return err
	}
	if p.Configuration.Digest != "dec965e6dabb48a245250fc27dbb56865562f6927f90bd636e9ff0f677daf066" {
		return fmt.Errorf("global configuration drifted")
	}
	if !equalStrings(p.RecordedCards, []string{"B17", "B24"}) || !equalStrings(sortedKeys(got.Recorders), p.RecordedCards) {
		return fmt.Errorf("recorded cards/recorders do not match B17/B24")
	}
	for card, recorder := range got.Recorders {
		want := expectedRecordings[card]
		source := "engdocs/design/plug/spec/behaviors/content.md#B17"
		if card == "B24" {
			source = "engdocs/design/plug/spec/behaviors/surfaces.md#B24"
		}
		if recorder.Disposition != (dispositionEvidence{Status: "keep", Source: source, Scope: "recorded-reference-vector"}) {
			return fmt.Errorf("%s disposition is missing or mismatched", card)
		}
		if len(recorder.SourceTests) != want.sourceCount || len(recorder.Commands) != 2 || sourceEvidenceDigest(recorder.SourceTests, recorder.Commands) != want.sourceEvidenceSHA || len(recorder.SourceFiles) != 1 {
			return fmt.Errorf("%s source tests, commands or sources are missing or mismatched", card)
		}
		file := recorder.SourceFiles[0]
		if file.Path != want.sourcePath || file.SHA256 != want.sourceSHA || file.Content == "" || digest([]byte(file.Content)) != file.SHA256 {
			return fmt.Errorf("%s durable recorder source is missing or mismatched", card)
		}
		if err := validateDigestEvidence(card+" configuration", recorder.Configuration); err != nil {
			return err
		}
		if err := validateDigestEvidence(card+" inputs", recorder.Inputs); err != nil {
			return err
		}
		if recorder.Configuration.Digest != want.configurationDigest || recorder.Inputs.Digest != want.inputDigest {
			return fmt.Errorf("%s configuration/input evidence drifted", card)
		}
		raw, err := canonicalJSON([]byte(recorder.RawJSON))
		if err != nil || string(raw) != recorder.RawJSON || recorder.Digest != want.digest || recorder.RawDigest != want.digest || digest(raw) != want.digest {
			return fmt.Errorf("%s raw bytes/digest are missing or mismatched", card)
		}
		if recorder.ArtifactSHA256 != want.artifactSHA || recorder.RepeatArtifactSHA256 != want.artifactSHA {
			return fmt.Errorf("%s independent artifact hashes differ", card)
		}
		var vector map[string]json.RawMessage
		if err := json.Unmarshal(raw, &vector); err != nil {
			return fmt.Errorf("%s raw vector: %w", card, err)
		}
		if string(vector["card"]) != `"`+card+`"` || string(vector["engine"]) != `"embedded-dolt"` {
			return fmt.Errorf("%s vector identity differs", card)
		}
		if recorder.Projection.Format != "plug-recorded-projection/1" || !equalStrings(recorder.Projection.Paths, want.projectionPaths) {
			return fmt.Errorf("%s projection declaration is missing or mismatched", card)
		}
		selected := make(map[string]json.RawMessage)
		for _, path := range recorder.Projection.Paths {
			value, ok := vector[path]
			if !ok || bytes.Equal(value, []byte("null")) {
				return fmt.Errorf("%s projection misses %s", card, path)
			}
			selected[path] = value
		}
		projection, err := json.Marshal(selected)
		if err != nil {
			return err
		}
		projection, err = canonicalJSON(projection)
		if err != nil || string(projection) != recorder.Projection.JSON || digest(projection) != want.projectionDigest || recorder.Projection.Digest != want.projectionDigest || recorder.ProjectionDigest != want.projectionDigest {
			return fmt.Errorf("%s projection bytes/digests are missing or mismatched", card)
		}
		bound := map[string]any{
			"card": card, "sourceCommit": p.SourceCommit, "binarySha256": p.Binary.SHA256,
			"recorder": p.Recorder, "driver": p.Driver, "schema": p.Schema,
			"sources":                   []map[string]string{{"path": file.Path, "sha256": file.SHA256}},
			"disposition":               recorder.Disposition,
			"configurationDigest":       recorder.Configuration.Digest,
			"globalConfigurationDigest": p.Configuration.Digest,
			"inputDigest":               recorder.Inputs.Digest,
			"rawDigest":                 recorder.RawDigest, "projectionDigest": recorder.ProjectionDigest,
		}
		boundJSON, err := json.Marshal(bound)
		if err != nil {
			return err
		}
		boundJSON, err = canonicalJSON(boundJSON)
		if err != nil {
			return err
		}
		repeated := recorder.RepeatedProvenance
		if repeated.First != string(boundJSON) || repeated.Repeat != string(boundJSON) || repeated.FirstDigest != want.provenanceDigest || repeated.RepeatDigest != want.provenanceDigest || digest(boundJSON) != want.provenanceDigest {
			return fmt.Errorf("%s repeated provenance bytes/digests are missing or not bound to inputs", card)
		}
	}
	return nil
}

func sortedKeys(values map[string]recordedCard) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestRecordedGoldensHavePinnedProvenance(t *testing.T) {
	got, _ := readManifest(t)
	if got.Provenance.SourceCommit != pinnedCommit || got.Provenance.SourceRef != "refs/tags/beads-base" {
		t.Fatalf("source pin = %q %q", got.Provenance.SourceRef, got.Provenance.SourceCommit)
	}
	if got.Provenance.Binary.SHA256 != pinnedBinarySHA256 || got.Provenance.Binary.Version == "" || got.Provenance.Binary.Path == "" || got.Provenance.Binary.BuildCommand == "" {
		t.Fatalf("incomplete binary provenance: %+v", got.Provenance.Binary)
	}
	requireDigest(t, "binary SHA-256", got.Provenance.Binary.SHA256)
	if got.Provenance.Recorder.Identity == "" || got.Provenance.Recorder.Version == "" || got.Provenance.Driver.Version != pinnedDriverVersion {
		t.Fatalf("incomplete recorder/driver provenance: %+v %+v", got.Provenance.Recorder, got.Provenance.Driver)
	}
	cards := append([]string(nil), got.Provenance.RecordedCards...)
	sort.Strings(cards)
	if !equalStrings(cards, []string{"B17", "B24"}) {
		t.Fatalf("recorded cards = %v", cards)
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
			if sourceEvidenceDigest(recorder.SourceTests, recorder.Commands) != want.sourceEvidenceSHA {
				t.Fatalf("recorder source evidence drifted: %s, want %s", sourceEvidenceDigest(recorder.SourceTests, recorder.Commands), want.sourceEvidenceSHA)
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
			if recorder.Digest != want.digest || recorder.RawDigest != want.digest {
				t.Fatalf("vector digest = %s/%s, want pinned %s", recorder.Digest, recorder.RawDigest, want.digest)
			}
			if recorder.ArtifactSHA256 != want.artifactSHA || recorder.RepeatArtifactSHA256 != want.artifactSHA {
				t.Fatalf("recording artifact hash drifted: %s %s", recorder.ArtifactSHA256, recorder.RepeatArtifactSHA256)
			}
			if recorder.ProjectionDigest != want.projectionDigest || recorder.Configuration.Digest != want.configurationDigest || recorder.Inputs.Digest != want.inputDigest || recorder.RepeatedProvenance.FirstDigest != want.provenanceDigest {
				t.Fatalf("provenance evidence drifted for %s", card)
			}
			requireDigest(t, "vector digest", recorder.Digest)
			requireDigest(t, "first artifact digest", recorder.ArtifactSHA256)
			requireDigest(t, "repeat artifact digest", recorder.RepeatArtifactSHA256)
			if recorder.ArtifactSHA256 != recorder.RepeatArtifactSHA256 {
				t.Fatal("independent recording artifacts differ")
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
			if card == "B17" {
				outcomes, ok := vector["outcomes"].(map[string]any)
				if !ok || outcomes["ephemeralMetadataCAS"] != "PASS" || outcomes["durableHistory"] != "PASS" {
					t.Fatalf("B17 history outcomes were not recorded: %#v", vector["outcomes"])
				}
			}
			if strings.Contains(strings.ToLower(recorder.RawJSON), "plug") {
				t.Fatal("reference vector contains Plug-derived expected data")
			}
		})
	}
}

func TestRecordedManifestRejectsMissingProvenanceOrDispositionEvidence(t *testing.T) {
	paths := []string{
		"provenance.recorder", "provenance.recorder.identity", "provenance.recorder.version",
		"provenance.recorder.source", "provenance.recorder.sha256",
		"provenance.driver", "provenance.driver.name", "provenance.driver.version",
		"provenance.schema", "provenance.schema.mainCursor", "provenance.schema.ignoredCursor",
		"provenance.schema.mainDigest", "provenance.schema.ignoredDigest", "provenance.schema.cursorSource",
		"provenance.configuration", "provenance.configuration.value", "provenance.configuration.digest",
		"provenance.recordedCards",
	}
	for _, card := range []string{"B17", "B24"} {
		for _, suffix := range []string{
			"sourceFiles", "sourceTests", "commands", "rawDigest",
			"configuration", "configuration.value", "configuration.digest",
			"inputs", "inputs.value", "inputs.digest", "inputs.value.clock",
			"inputs.value.clock.mode", "inputs.value.ids", "inputs.value.ids.explicit", "inputs.value.graph",
			"inputs.value.graph.seed", "inputs.value.graph.nodes", "inputs.value.graph.edges",
			"projection", "projection.format", "projection.paths", "projection.json", "projection.digest", "projectionDigest",
			"repeatedProvenance", "repeatedProvenance.first", "repeatedProvenance.repeat",
			"repeatedProvenance.firstDigest", "repeatedProvenance.repeatDigest",
			"disposition", "disposition.status", "disposition.source", "disposition.scope",
		} {
			paths = append(paths, "recorders."+card+"."+suffix)
		}
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal(goldenFile, &raw); err != nil {
				t.Fatal(err)
			}
			parts := strings.Split(path, ".")
			object := raw
			for _, part := range parts[:len(parts)-1] {
				object = object[part].(map[string]any)
			}
			delete(object, parts[len(parts)-1])
			data, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeManifest(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateManifest(got); err == nil {
				t.Fatalf("accepted manifest missing %s evidence", path)
			}
		})
	}
}

func TestRepeatedProvenanceBindsEvidenceAndRequiresBothRuns(t *testing.T) {
	for _, card := range []string{"B17", "B24"} {
		for _, field := range []string{"recorder", "sources", "driver", "schema", "disposition", "configurationDigest", "globalConfigurationDigest", "inputDigest", "rawDigest", "projectionDigest"} {
			t.Run(card+"/"+field, func(t *testing.T) {
				got := decodeGoldenManifest(t)
				recorder := got.Recorders[card]
				var bound map[string]any
				if err := json.Unmarshal([]byte(recorder.RepeatedProvenance.First), &bound); err != nil {
					t.Fatal(err)
				}
				delete(bound, field)
				data, err := json.Marshal(bound)
				if err != nil {
					t.Fatal(err)
				}
				recorder.RepeatedProvenance = repeatedProvenanceEvidence{
					First: string(data), Repeat: string(data), FirstDigest: digest(data), RepeatDigest: digest(data),
				}
				got.Recorders[card] = recorder
				if err := validateManifest(got); err == nil {
					t.Fatalf("accepted rehashed provenance without %s", field)
				}
			})
		}
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
