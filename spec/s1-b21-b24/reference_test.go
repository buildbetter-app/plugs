package s1b21b24

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The raw Beads twins are retained as separate embedded files. They were
// recorded in independently initialized disposable stores from the pinned
// executable; this test derives the stable projection from each raw command
// transcript instead of trusting a recorder summary.
//
//go:embed reference-a.json reference-b.json record_reference.py
var referenceFiles embed.FS

const (
	beadsCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	beadsTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
	beadsBinary = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
)

type beadsReference struct {
	Schema              string              `json:"schema"`
	Recording           string              `json:"recording"`
	Source              beadsSource         `json:"source"`
	Inputs              map[string]any      `json:"inputs"`
	InputSHA256         string              `json:"inputSha256"`
	ProjectionPaths     []string            `json:"projectionPaths"`
	ProjectionOmissions []string            `json:"projectionOmissions"`
	Unsupported         map[string]string   `json:"unsupported"`
	RecorderSHA256      string              `json:"recorderSha256"`
	Decisions           map[string][]string `json:"decisions"`
	Run                 beadsRun            `json:"run"`
}

type beadsSource struct {
	Commit                   string `json:"commit"`
	Tree                     string `json:"tree"`
	Engine                   string `json:"engine"`
	BinarySHA256             string `json:"binarySha256"`
	SourceArchiveSHA256      string `json:"sourceArchiveSha256"`
	BuildInfoSHA256          string `json:"buildInfoSha256"`
	SourceVerificationSHA256 string `json:"sourceVerificationSha256"`
	BuildCommandSHA256       string `json:"buildCommandSha256"`
}

type beadsRun struct {
	Workspace        string         `json:"workspace"`
	RawJSON          string         `json:"rawJSON"`
	RawSHA256        string         `json:"rawSha256"`
	Projection       map[string]any `json:"projection"`
	ProjectionSHA256 string         `json:"projectionSha256"`
}

type beadsCommand struct {
	Step   string   `json:"step"`
	Argv   []string `json:"argv"`
	Stdin  *string  `json:"stdin"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	Stderr string   `json:"stderr"`
}

func referenceSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func referenceJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func loadBeadsReference(t *testing.T, name string) (beadsReference, []byte) {
	t.Helper()
	raw, err := referenceFiles.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var value beadsReference
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return value, raw
}

func decodeCommands(t *testing.T, run beadsRun) []beadsCommand {
	t.Helper()
	var commands []beadsCommand
	decoder := json.NewDecoder(strings.NewReader(run.RawJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&commands); err != nil {
		t.Fatalf("decode raw Beads transcript: %v", err)
	}
	if len(commands) != 14 {
		t.Fatalf("raw Beads transcript has %d commands, want 14", len(commands))
	}
	wantSteps := []string{"init", "anchor", "beforeOrderedList", "beforeOrderedHistory", "orderedBatch", "orderedAnchor", "afterOrderedList", "afterOrderedHistory", "refusedBatch", "afterRefusedList", "afterRefusedHistory", "closeBatch", "closedAnchor", "afterCloseHistory"}
	for i, command := range commands {
		if command.Step != wantSteps[i] || len(command.Argv) < 6 || filepath.Base(command.Argv[0]) != "reference-engine" {
			t.Fatalf("command %d provenance changed: %+v", i, command)
		}
		if !reflect.DeepEqual(command.Argv[1:5], []string{"--sandbox", "--actor", "cardb21", "--json"}) {
			t.Fatalf("command %d actor or JSON mode changed: %+v", i, command.Argv)
		}
		if command.Step != "refusedBatch" && (command.Exit != 0 || command.Stderr != "") {
			t.Fatalf("command %s unexpectedly failed: %+v", command.Step, command)
		}
		if command.Step == "refusedBatch" && (command.Exit == 0 || !strings.Contains(command.Stderr, "b21-missing")) {
			t.Fatalf("command %s lost its typed refusal: %+v", command.Step, command)
		}
	}
	return commands
}

func referenceProjection(t *testing.T, run beadsRun) map[string]any {
	t.Helper()
	commands := decodeCommands(t, run)
	byStep := make(map[string]beadsCommand, len(commands))
	for _, command := range commands {
		byStep[command.Step] = command
	}
	decode := func(step string) any {
		var value any
		if err := json.Unmarshal([]byte(byStep[step].Stdout), &value); err != nil {
			t.Fatalf("decode %s output: %v", step, err)
		}
		return value
	}
	rows := func(step string) []any {
		value, ok := decode(step).([]any)
		if !ok {
			t.Fatalf("%s output is not a row array", step)
		}
		return value
	}
	single := func(step string) map[string]any {
		items := rows(step)
		if len(items) != 1 {
			t.Fatalf("%s output has %d rows, want one", step, len(items))
		}
		value, ok := items[0].(map[string]any)
		if !ok {
			t.Fatalf("%s row is not an object", step)
		}
		return value
	}
	ordered, ok := decode("orderedBatch").(map[string]any)
	if !ok {
		t.Fatal("ordered batch output is not an object")
	}
	results, ok := ordered["results"].([]any)
	if !ok || len(results) != 4 {
		t.Fatalf("ordered batch results=%v", ordered["results"])
	}
	createdID := results[1].(map[string]any)["target"].(string)
	roles := []any{"anchor", "created", "anchor", "anchor"}
	for i, result := range results {
		row := result.(map[string]any)
		if row["line"] != float64(i+1) {
			t.Fatalf("ordered result line %d: %+v", i, row)
		}
		want := "b21-anchor"
		if i == 1 {
			want = createdID
		}
		if row["target"] != want {
			t.Fatalf("ordered result target %d=%v want %v", i, row["target"], want)
		}
	}
	anchor := single("orderedAnchor")
	createdTitle := ""
	for _, row := range rows("afterOrderedList") {
		item := row.(map[string]any)
		if item["id"] == createdID {
			createdTitle, _ = item["title"].(string)
		}
	}
	if createdTitle == "" {
		t.Fatal("created row absent from ordered list")
	}
	return map[string]any{
		"orderedAnchorTitle":              anchor["title"],
		"orderedAnchorPriority":           anchor["priority"],
		"orderedCreatedTitle":             createdTitle,
		"orderedItemCount":                ordered["operations"],
		"orderedResultRoles":              roles,
		"orderedRowDelta":                 float64(len(rows("afterOrderedList")) - len(rows("beforeOrderedList"))),
		"orderedHistoryDelta":             float64(len(rows("afterOrderedHistory")) - len(rows("beforeOrderedHistory"))),
		"refusedBatchExitNonzero":         byStep["refusedBatch"].Exit != 0,
		"refusedBatchChangedState":        !reflect.DeepEqual(rows("afterRefusedList"), rows("afterOrderedList")),
		"refusedHistoryChanged":           !reflect.DeepEqual(rows("afterRefusedHistory"), rows("afterOrderedHistory")),
		"referenceBatchCloseStatus":       single("closedAnchor")["status"],
		"referenceBatchCloseHistoryDelta": float64(len(rows("afterCloseHistory")) - len(rows("afterRefusedHistory"))),
	}
}

func validateReference(t *testing.T, name string) beadsReference {
	t.Helper()
	value, raw := loadBeadsReference(t, name)
	if value.Schema != "plug.s1-b21-b24.beads-reference.v1" || value.Source.Commit != beadsCommit || value.Source.Tree != beadsTree || value.Source.Engine != "embedded-dolt" || value.Source.BinarySHA256 != beadsBinary {
		t.Fatalf("reference provenance changed: %+v", value)
	}
	for _, digest := range []string{value.Source.SourceArchiveSHA256, value.Source.BuildInfoSHA256, value.Source.SourceVerificationSHA256, value.Source.BuildCommandSHA256, value.InputSHA256, value.RecorderSHA256} {
		if len(digest) != 64 {
			t.Fatalf("reference digest is not SHA-256: %q", digest)
		}
	}
	if value.Run.Workspace == "" || referenceSHA([]byte(value.Run.RawJSON)) != value.Run.RawSHA256 {
		t.Fatalf("raw recording digest or workspace is invalid: %+v", value.Run)
	}
	if !reflect.DeepEqual(referenceProjection(t, value.Run), value.Run.Projection) || referenceSHA(referenceJSON(value.Run.Projection)) != value.Run.ProjectionSHA256 {
		t.Fatalf("semantic projection for %s is not independently reproducible", name)
	}
	recorder, err := referenceFiles.ReadFile("record_reference.py")
	if err != nil {
		t.Fatal(err)
	}
	if referenceSHA(recorder) != value.RecorderSHA256 {
		t.Fatalf("recorder digest changed: got %s want %s", referenceSHA(recorder), value.RecorderSHA256)
	}
	if len(value.ProjectionPaths) != len(value.Run.Projection) || len(value.ProjectionOmissions) == 0 || len(value.Unsupported) < 3 {
		t.Fatalf("reference limits are incomplete: %+v", value)
	}
	for _, card := range []string{"B21", "B22", "B23", "B24", "I1"} {
		if len(value.Decisions[card]) == 0 {
			t.Fatalf("missing semantic decision IDs for %s", card)
		}
	}
	if len(raw) == 0 {
		t.Fatal("empty embedded reference")
	}
	return value
}

func TestS1PinnedBeadsTwinsAndSemanticDecisions(t *testing.T) {
	first := validateReference(t, "reference-a.json")
	repeat := validateReference(t, "reference-b.json")
	if first.Recording == repeat.Recording || first.Run.Workspace == repeat.Run.Workspace {
		t.Fatal("paired Beads recordings did not use independent stores")
	}
	if !reflect.DeepEqual(first.Run.Projection, repeat.Run.Projection) {
		t.Fatalf("paired Beads semantic projections differ: %v vs %v", first.Run.Projection, repeat.Run.Projection)
	}
	firstCommands := decodeCommands(t, first.Run)
	repeatCommands := decodeCommands(t, repeat.Run)
	firstBatch := firstCommands[4].Stdout
	repeatBatch := repeatCommands[4].Stdout
	if firstBatch == repeatBatch {
		t.Fatal("independent Beads runs unexpectedly retained identical minted IDs")
	}
	t.Logf("S1 reference PASS: pinned Beads twins agree on ordered batch/rollback projection; B24 remains the preserved untimed deviation")
}
