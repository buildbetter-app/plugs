package recordedextension

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

// These checks cover only the independent extension package. The merged B17/B24
// validator and manifest remain in //spec/recorded:goldens_test unchanged.

//go:embed extension.json
var extensionFile []byte

//go:embed build50_record.py
var recorderSource []byte

//go:embed provenance.py
var projectorSource []byte

const (
	pinnedCommit       = "9d6fff760b7a314b476539198504d77ce0959656"
	pinnedBinarySHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
	pinnedDriver       = "github.com/dolthub/dolt/go@v0.40.5-0.20260806213044-796d07497741"
	pinnedManifestSHA  = "31f68e58ea444c90c0780b44beec91dc6bcd7255c3f2c859b146d4337c01708d"
)

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// These digests pin the independent observations as well as the provenance
// contract, so deleting evidence and rehashing it cannot pass validation.
var cardPins = map[string]struct{ raw, artifact, projection, provenance, commands string }{
	"B07": {"70bca82d100411002710230cd289028d2f657617ee52ee35c56edda627d4dc12", "93ea20b92e2347d51618a99931c34ba8159899fd76a68da3956ce9614d493ac4", "1aa92a623518d50cf13ccf4c0d08afdf064b3a8ed8f4c42a2ae420f071170b65", "6d79a864ae6572127478e45b278415677b106aec4f487b11cce9f31191b1a880", "cf7f91eebd19de9addc8e18b2dca18afaa486c87956477c7e242a1b6e1ebe23e"},
	"B25": {"c58567d74e759df673b90e14d04b004ba155bcd24b0ca51959365d6be7fb25ec", "05703a0500bec117963b002392e49a1ccf952bd122df39eb82a6c3676043b0f2", "40cd56ebfd286a8caecd7a3a93b918254badc3888f3ccd5cb133cf21eaf89ce0", "bcecc9ee06511686039aa9ee3f9ee335210bca5dad27ec12ae27d90840197ba3", "e42a19ab7e8eb7be9ce6a21ea8f2314b2599d2ed91100fa65b4a4ddd2f2162c3"},
}

type manifest struct {
	Format     string                `json:"format"`
	Provenance globalProvenance      `json:"provenance"`
	Recorders  map[string]cardRecord `json:"recorders"`
}

type globalProvenance struct {
	SourceRepository string         `json:"sourceRepository"`
	SourceRef        string         `json:"sourceRef"`
	SourceCommit     string         `json:"sourceCommit"`
	SourceTree       string         `json:"sourceTree"`
	Binary           binaryInfo     `json:"binary"`
	Toolchain        toolchainInfo  `json:"toolchain"`
	Runtime          string         `json:"runtime"`
	RecordedCards    []string       `json:"recordedCards"`
	ReferenceEngine  string         `json:"referenceEngine"`
	Clock            clockInfo      `json:"clock"`
	Recorder         sourceInfo     `json:"recorder"`
	Projector        sourceInfo     `json:"projector"`
	Driver           driverInfo     `json:"driver"`
	Schema           schemaInfo     `json:"schema"`
	Configuration    digestEvidence `json:"configuration"`
}

type binaryInfo struct {
	BuildCommand string `json:"buildCommand"`
	Path         string `json:"path"`
	Version      string `json:"version"`
	SHA256       string `json:"sha256"`
}

type toolchainInfo struct {
	GoVersion       string            `json:"goVersion"`
	PythonVersion   string            `json:"pythonVersion"`
	BuildInfoSHA256 string            `json:"buildInfoSHA256"`
	Settings        map[string]string `json:"settings"`
}

type sourceInfo struct {
	Identity string `json:"identity"`
	Version  string `json:"version"`
	Source   string `json:"source"`
	SHA256   string `json:"sha256"`
}

type driverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type schemaInfo struct {
	MainCursor    int    `json:"mainCursor"`
	IgnoredCursor int    `json:"ignoredCursor"`
	MainDigest    string `json:"mainDigest"`
	IgnoredDigest string `json:"ignoredDigest"`
	CursorSource  string `json:"cursorSource"`
}

type clockInfo struct {
	Mode          string   `json:"mode"`
	FieldsOmitted []string `json:"fieldsOmitted"`
	Note          string   `json:"note"`
}

type digestEvidence struct {
	Value  json.RawMessage `json:"value"`
	Digest string          `json:"digest"`
}

type cardRecord struct {
	SourceTests          []string           `json:"sourceTests"`
	SourceRefs           []sourceRef        `json:"sourceRefs"`
	Commands             []string           `json:"commands"`
	ProjectionCommands   []string           `json:"projectionCommands"`
	RawJSON              string             `json:"rawJSON"`
	Digest               string             `json:"digest"`
	ArtifactSHA256       string             `json:"artifactSha256"`
	RepeatArtifactSHA256 string             `json:"repeatArtifactSha256"`
	Disposition          disposition        `json:"disposition"`
	Configuration        digestEvidence     `json:"configuration"`
	Inputs               digestEvidence     `json:"inputs"`
	Projection           projection         `json:"projection"`
	RawDigest            string             `json:"rawDigest"`
	ProjectionDigest     string             `json:"projectionDigest"`
	RepeatedProvenance   repeatedProvenance `json:"repeatedProvenance"`
}

type sourceRef struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type disposition struct {
	Status string `json:"status"`
	Source string `json:"source"`
	Scope  string `json:"scope"`
}

type projection struct {
	Format string   `json:"format"`
	Paths  []string `json:"paths"`
	JSON   string   `json:"json"`
	Digest string   `json:"digest"`
}

type repeatedProvenance struct {
	First        string `json:"first"`
	Repeat       string `json:"repeat"`
	FirstDigest  string `json:"firstDigest"`
	RepeatDigest string `json:"repeatDigest"`
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func marshalCanonical(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func canonicalJSON(value []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, err
	}
	return marshalCanonical(decoded)
}

func decode(data []byte) (manifest, error) {
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

func requireDigest(label, value string) error {
	if !digestPattern.MatchString(value) {
		return fmt.Errorf("%s is not a lowercase SHA-256 digest: %q", label, value)
	}
	return nil
}

func validateEvidence(label string, evidence digestEvidence) error {
	if len(evidence.Value) == 0 {
		return fmt.Errorf("%s value missing", label)
	}
	canonical, err := canonicalJSON(evidence.Value)
	if err != nil {
		return fmt.Errorf("%s value: %w", label, err)
	}
	if err := requireDigest(label, evidence.Digest); err != nil {
		return err
	}
	if digest(canonical) != evidence.Digest {
		return fmt.Errorf("%s digest mismatch", label)
	}
	return nil
}

func validateManifest(got manifest) error {
	if got.Format != "plug-recorded-goldens-extension/1" {
		return fmt.Errorf("format=%q", got.Format)
	}
	p := got.Provenance
	if p.SourceRepository != "beads-base provenance tag in buildbetter-app/plugs" || p.SourceRef != "refs/tags/beads-base" || p.SourceCommit != pinnedCommit || p.Runtime == "" || p.ReferenceEngine != "embedded-dolt" {
		return fmt.Errorf("source provenance is incomplete")
	}
	if p.SourceTree != "cde121d6e4119e454857ad1c3ca293e2791be840" || p.Toolchain.GoVersion != "go1.26.10" || p.Toolchain.PythonVersion != "3.12.3" || p.Toolchain.BuildInfoSHA256 != "2edc35ea3a2f0d50f708bda2271742494191f3288466ba246f6a8d0502f64d50" || len(p.Toolchain.Settings) != 3 || p.Toolchain.Settings["GOOS"] != "linux" || p.Toolchain.Settings["GOARCH"] != "amd64" || p.Toolchain.Settings["CGO_ENABLED"] != "1" {
		return fmt.Errorf("source tree or toolchain provenance is incomplete or drifted")
	}
	if p.Binary.SHA256 != pinnedBinarySHA256 || p.Binary.Path == "" || p.Binary.Version == "" || p.Binary.BuildCommand == "" {
		return fmt.Errorf("binary provenance is incomplete")
	}
	if p.Recorder.Identity != "build50-reference-recorder" || p.Recorder.Version != "1" || p.Recorder.Source != "spec/recorded/extension/build50_record.py" || p.Recorder.SHA256 != digest(recorderSource) {
		return fmt.Errorf("recorder provenance is incomplete or drifted")
	}
	if p.Projector.Identity != "build50-reference-provenance" || p.Projector.Version != "1" || p.Projector.Source != "spec/recorded/extension/provenance.py" || p.Projector.SHA256 != digest(projectorSource) {
		return fmt.Errorf("projector provenance is incomplete or drifted")
	}
	if p.Driver.Name != "embedded-dolt" || p.Driver.Version != pinnedDriver {
		return fmt.Errorf("driver provenance is incomplete or drifted")
	}
	if p.Clock.Mode != "runtime-non-temporal-slice" || !equalStrings(p.Clock.FieldsOmitted, []string{"timestamps", "revision hashes", "generated relation IDs"}) || p.Clock.Note == "" {
		return fmt.Errorf("clock provenance is incomplete")
	}
	if p.Schema.MainCursor != 72 || p.Schema.IgnoredCursor != 28 || p.Schema.MainDigest != "c649854f6d67016df0db1c3b324679a41e090b8fad0ae5ac5d94cf72d5e8f0f2" || p.Schema.IgnoredDigest != "31abcfd3374b7e7ae7b95727785a1591107cac63cbf5db59ea6685350c05b470" || p.Schema.CursorSource != "pinned migration inventory; CLI does not expose embedded SQL" {
		return fmt.Errorf("schema provenance is incomplete")
	}
	if err := validateEvidence("global configuration", p.Configuration); err != nil {
		return err
	}
	if p.Configuration.Digest != "dfeba68ab7a13e91481c014f030eaef84a9aed98f4d30f57df033a9b222ef00b" {
		return fmt.Errorf("global configuration drifted")
	}
	wantCards := []string{"B07", "B25"}
	if !equalStrings(p.RecordedCards, wantCards) || !equalStrings(sortedKeys(got.Recorders), wantCards) {
		return fmt.Errorf("recorded cards do not match B07/B25")
	}
	for _, card := range wantCards {
		recorder := got.Recorders[card]
		if err := validateCard(card, recorder, p); err != nil {
			return err
		}
	}
	return nil
}

func validateCard(card string, recorder cardRecord, global globalProvenance) error {
	if len(recorder.SourceTests) == 0 || len(recorder.SourceRefs) == 0 || len(recorder.Commands) != 2 || len(recorder.ProjectionCommands) != 2 {
		return fmt.Errorf("%s source/command evidence is incomplete", card)
	}
	commands, err := marshalCanonical(map[string][]string{"recording": recorder.Commands, "projection": recorder.ProjectionCommands})
	if err != nil || digest(commands) != cardPins[card].commands {
		return fmt.Errorf("%s execution commands differ from recorded commands", card)
	}
	for _, ref := range recorder.SourceRefs {
		if ref.Path == "" {
			return fmt.Errorf("%s source reference path is blank", card)
		}
		if err := requireDigest(card+" source reference", ref.SHA256); err != nil {
			return err
		}
	}
	source := "engdocs/design/plug/spec/behaviors/work.md#B07"
	if card == "B25" {
		source = "engdocs/design/plug/spec/behaviors/surfaces.md#B25"
	}
	if recorder.Disposition != (disposition{Status: "keep", Source: source, Scope: "recorded-reference-vector"}) {
		return fmt.Errorf("%s disposition is incomplete", card)
	}
	if err := validateEvidence(card+" configuration", recorder.Configuration); err != nil {
		return err
	}
	if err := validateEvidence(card+" inputs", recorder.Inputs); err != nil {
		return err
	}
	raw, err := canonicalJSON([]byte(recorder.RawJSON))
	pin := cardPins[card]
	if err != nil || string(raw) != recorder.RawJSON || recorder.Digest != pin.raw || digest(raw) != recorder.Digest || recorder.RawDigest != recorder.Digest {
		return fmt.Errorf("%s raw vector is not canonical or digest-bound", card)
	}
	if recorder.ArtifactSHA256 != pin.artifact || recorder.ArtifactSHA256 != recorder.RepeatArtifactSHA256 {
		return fmt.Errorf("%s recorder artifacts are not byte-identical", card)
	}
	var vector map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vector); err != nil {
		return fmt.Errorf("%s raw vector: %w", card, err)
	}
	if string(vector["card"]) != `"`+card+`"` || string(vector["engine"]) != `"embedded-dolt"` {
		return fmt.Errorf("%s vector identity mismatch", card)
	}
	for _, evidence := range []struct {
		name  string
		value any
	}{
		{"sourceTests", recorder.SourceTests}, {"configuration", json.RawMessage(recorder.Configuration.Value)},
		{"inputs", json.RawMessage(recorder.Inputs.Value)},
	} {
		value, err := json.Marshal(evidence.value)
		if err != nil {
			return err
		}
		value, err = canonicalJSON(value)
		if err != nil || !bytes.Equal(vector[evidence.name], value) {
			return fmt.Errorf("%s %s differs from raw inputs", card, evidence.name)
		}
	}
	if !equalStrings(recorder.Projection.Paths, []string{"card", "engine", "observations"}) {
		return fmt.Errorf("%s projection declaration drifted", card)
	}
	selected := map[string]json.RawMessage{}
	for _, path := range recorder.Projection.Paths {
		value, ok := vector[path]
		if !ok || bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("%s projection misses %s", card, path)
		}
		selected[path] = value
	}
	projectionBytes, err := json.Marshal(selected)
	if err != nil {
		return err
	}
	projectionBytes, err = canonicalJSON(projectionBytes)
	if err != nil || recorder.Projection.Format != "plug-recorded-projection/1" || string(projectionBytes) != recorder.Projection.JSON || digest(projectionBytes) != recorder.Projection.Digest || recorder.ProjectionDigest != recorder.Projection.Digest || recorder.ProjectionDigest != pin.projection {
		return fmt.Errorf("%s projection is not derived from the raw vector", card)
	}
	bound := map[string]any{
		"card": card, "sourceCommit": global.SourceCommit, "sourceTree": global.SourceTree,
		"binarySha256": global.Binary.SHA256, "toolchain": global.Toolchain,
		"recorder": global.Recorder, "projector": global.Projector, "sources": recorder.SourceRefs,
		"driver": global.Driver, "schema": global.Schema, "disposition": recorder.Disposition,
		"configurationDigest": recorder.Configuration.Digest, "inputDigest": recorder.Inputs.Digest,
		"sourceTests": recorder.SourceTests, "commands": vector["commands"],
		"rawDigest": recorder.RawDigest, "projectionDigest": recorder.ProjectionDigest,
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
	if repeated.First != string(boundJSON) || repeated.Repeat != string(boundJSON) || repeated.FirstDigest != pin.provenance || repeated.RepeatDigest != pin.provenance || digest(boundJSON) != pin.provenance {
		return fmt.Errorf("%s repeated provenance is not bound to the evidence", card)
	}
	return nil
}

func sortedKeys(values map[string]cardRecord) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func readManifest(t *testing.T) manifest {
	t.Helper()
	if digest(extensionFile) != pinnedManifestSHA {
		t.Fatalf("extension manifest drifted: %s", digest(extensionFile))
	}
	got, err := decode(extensionFile)
	if err != nil {
		t.Fatalf("decode extension manifest: %v", err)
	}
	if err := validateManifest(got); err != nil {
		t.Fatalf("validate extension manifest: %v", err)
	}
	return got
}

func TestRecordedExtensionHasPinnedProvenance(t *testing.T) {
	got := readManifest(t)
	if got.Provenance.SourceCommit != pinnedCommit || got.Provenance.Binary.SHA256 != pinnedBinarySHA256 {
		t.Fatalf("source/binary pin drifted: %+v", got.Provenance)
	}
}

func TestRecordedExtensionVectorsReplayIndependentDigests(t *testing.T) {
	got := readManifest(t)
	for _, card := range got.Provenance.RecordedCards {
		t.Run(card, func(t *testing.T) {
			recorder := got.Recorders[card]
			if err := requireDigest(card+" vector", recorder.Digest); err != nil {
				t.Fatal(err)
			}
			if recorder.ArtifactSHA256 != recorder.RepeatArtifactSHA256 {
				t.Fatal("artifact repeat differs")
			}
			if strings.Contains(strings.ToLower(recorder.RawJSON), "plug") {
				t.Fatal("reference vector contains product-derived output")
			}
		})
	}
}

func TestRecordedExtensionRejectsTrailingData(t *testing.T) {
	if _, err := decode(append(append([]byte(nil), extensionFile...), []byte("{}")...)); err == nil {
		t.Fatal("extension decoder accepted trailing JSON")
	}
}

func TestRecordedExtensionIsEmbedded(t *testing.T) {
	if len(extensionFile) == 0 || len(recorderSource) == 0 || len(projectorSource) == 0 {
		t.Fatal("extension reference inputs were not embedded")
	}
}

func TestRecordedExtensionRejectsMissingProvenance(t *testing.T) {
	for _, path := range []string{
		"sourceRepository", "sourceRef", "sourceCommit", "sourceTree", "binary", "toolchain", "runtime",
		"recordedCards", "referenceEngine", "clock", "recorder", "projector", "driver", "schema", "configuration",
	} {
		t.Run("global/"+path, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(extensionFile, &value); err != nil {
				t.Fatal(err)
			}
			delete(value["provenance"].(map[string]any), path)
			requireRejectedManifest(t, value)
		})
	}
	for _, card := range []string{"B07", "B25"} {
		for _, path := range []string{
			"sourceTests", "sourceRefs", "commands", "projectionCommands", "rawJSON", "digest",
			"artifactSha256", "repeatArtifactSha256", "disposition", "configuration", "inputs", "projection",
			"rawDigest", "projectionDigest", "repeatedProvenance",
		} {
			t.Run(card+"/"+path, func(t *testing.T) {
				var value map[string]any
				if err := json.Unmarshal(extensionFile, &value); err != nil {
					t.Fatal(err)
				}
				delete(value["recorders"].(map[string]any)[card].(map[string]any), path)
				requireRejectedManifest(t, value)
			})
		}
	}
}

func requireRejectedManifest(t *testing.T, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decode(data)
	if err == nil && validateManifest(got) == nil {
		t.Fatal("incomplete or altered provenance was accepted")
	}
}

func TestRecordedExtensionRejectsAlteredEvidenceEvenAfterRehashing(t *testing.T) {
	for _, card := range []string{"B07", "B25"} {
		for _, field := range []string{"configuration", "inputs", "projection", "provenance", "commands", "source"} {
			t.Run(card+"/"+field, func(t *testing.T) {
				got := readManifest(t)
				recorded := got.Recorders[card]
				empty := digestEvidence{Value: json.RawMessage(`{}`), Digest: digest([]byte(`{}`))}
				switch field {
				case "configuration":
					recorded.Configuration = empty
				case "inputs":
					recorded.Inputs = empty
				case "projection":
					recorded.Projection.JSON = `{}`
					recorded.Projection.Digest = empty.Digest
					recorded.ProjectionDigest = empty.Digest
				case "provenance":
					recorded.RepeatedProvenance = repeatedProvenance{
						First: `{}`, Repeat: `{}`, FirstDigest: empty.Digest, RepeatDigest: empty.Digest,
					}
				case "commands":
					recorded.Commands[0] += " altered"
				case "source":
					recorded.SourceRefs[0].SHA256 = empty.Digest
				}
				got.Recorders[card] = recorded
				requireRejectedManifest(t, got)
			})
		}
	}
}
