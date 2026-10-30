package spec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/acceptance"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

var recordedExtension = flag.String("recorded-extension", "", "runfile path of the independently recorded reference extension")

const recordedExtensionSHA = "31f68e58ea444c90c0780b44beec91dc6bcd7255c3f2c859b146d4337c01708d"

type referenceVector struct {
	Card   string `json:"card"`
	Inputs struct {
		IDs struct {
			Explicit []string `json:"explicit"`
		} `json:"ids"`
	} `json:"inputs"`
	Commands []struct {
		Args []string `json:"args"`
		Exit int      `json:"exit"`
	} `json:"commands"`
	Observations json.RawMessage `json:"observations"`
}

func recordedVectors(t *testing.T) map[string]referenceVector {
	t.Helper()
	if *recordedExtension == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("Bazel must supply the recorded reference runfile")
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_SRCDIR"), *recordedExtension))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != recordedExtensionSHA {
		t.Fatal("recorded reference manifest changed without reviewed repinning")
	}
	var manifest struct {
		Recorders map[string]struct {
			RawJSON string `json:"rawJSON"`
			Digest  string `json:"digest"`
		} `json:"recorders"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil || len(manifest.Recorders) != 2 {
		t.Fatalf("recorded manifest: %v", err)
	}
	vectors := map[string]referenceVector{}
	for card, recording := range manifest.Recorders {
		sum := sha256.Sum256([]byte(recording.RawJSON))
		if hex.EncodeToString(sum[:]) != recording.Digest {
			t.Fatalf("%s reference vector digest mismatch", card)
		}
		var vector referenceVector
		if err := json.Unmarshal([]byte(recording.RawJSON), &vector); err != nil || vector.Card != card {
			t.Fatalf("%s reference vector: %v", card, err)
		}
		vectors[card] = vector
	}
	return vectors
}

func referenceArgument(t *testing.T, args []string, flag string) string {
	t.Helper()
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	t.Fatalf("reference sequence lacks %s in %v", flag, args)
	return ""
}

func observedEdges(doc pipeline.Document) []map[string]string {
	edges := make([]map[string]string, 0, len(doc.Links))
	for _, edge := range doc.Links {
		edges = append(edges, map[string]string{"from": doc.ID, "to": edge.To.ID, "kind": edge.Kind})
	}
	return edges
}

func observedRelations(t *testing.T, doc pipeline.Document) map[string]any {
	t.Helper()
	labels, err := json.Marshal(doc.Fields["labels"])
	if err != nil {
		t.Fatal(err)
	}
	var labelValues []string
	if err := json.Unmarshal(labels, &labelValues); err != nil || labelValues == nil {
		t.Fatalf("persisted labels missing: %s, %v", labels, err)
	}
	comments := make([]string, 0, len(doc.Comments))
	for _, comment := range doc.Comments {
		comments = append(comments, comment.Body)
	}
	return map[string]any{"labels": labelValues, "dependencies": observedEdges(doc),
		"comments": comments, "commentCount": len(doc.Comments)}
}

// Only the recorded non-temporal operation vocabulary is lowered here. Graph
// setup goes through the existing Writer; reads and scalar/comment writes go
// through the existing RPC trace. No alternate handler or storage is installed.
func replayRecordedVector(t *testing.T, vector referenceVector) ([]acceptance.Frame, json.RawMessage) {
	t.Helper()
	f := newStack(t, vector.Inputs.IDs.Explicit)
	views := []pipeline.Document{}
	for step, command := range vector.Commands {
		args := command.Args
		if len(args) == 0 || command.Exit != 0 {
			t.Fatalf("reference command %d was not successful", step)
		}
		switch args[0] {
		case "init":
			if step != 0 {
				t.Fatal("reference initialized a second store")
			}
		case "create":
			id := referenceArgument(t, args, "--id")
			title := referenceArgument(t, args, "--title")
			response := f.create(t, title, map[string]any{"priority": 2})
			assertPaths(t, response, map[string]any{"/result/results/0/ref/id": id})
		case "dep":
			kind, operation := "blocks", model.OpLink
			if args[1] == "remove" {
				operation = model.OpUnlink
			} else if args[1] == "add" {
				kind = referenceArgument(t, args, "--type")
			} else {
				t.Fatalf("unsupported recorded edge operation %v", args)
			}
			f.core.apply(t, "reference-edge-"+string(rune('A'+step)), model.ChangeSet{Ops: []model.Op{{
				Kind: operation, Link: &model.LinkOp{From: model.RefByID(args[2], model.Natural64(0)),
					To: model.RefByID(args[3], model.Natural64(0)), LinkKind: kind},
			}}})
		case "label":
			if args[1] != "add" {
				t.Fatalf("unsupported recorded label operation %v", args)
			}
			doc := f.core.document(t, args[2])
			var labels []string
			if value, ok := doc.Fields["labels"]; ok {
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &labels); err != nil {
					t.Fatal(err)
				}
			}
			labels = append(labels, args[3])
			f.success(t, "bead.update", map[string]any{"id": idRef(args[2]), "fields": map[string]any{"labels": labels}})
		case "comments":
			if args[1] != "add" {
				t.Fatalf("unsupported recorded comment operation %v", args)
			}
			f.success(t, "bead.comment", map[string]any{"id": idRef(args[2]), "body": args[3]})
		case "update":
			field, value := args[2][2:], any(args[3])
			if field == "priority" {
				value = json.Number(args[3])
			}
			f.success(t, "bead.update", map[string]any{"id": idRef(args[1]), "fields": map[string]any{field: value}})
		case "show":
			f.success(t, "bead.get", map[string]any{"id": idRef(args[1])})
			views = append(views, f.core.document(t, args[1]))
		default:
			t.Fatalf("unsupported recorded command %v", args)
		}
	}
	var observed any
	switch vector.Card {
	case "B07":
		if len(views) != 3 {
			t.Fatalf("B07 observed %d detail reads", len(views))
		}
		observed = map[string]any{"afterSameTypeReadds": observedEdges(views[0]),
			"afterRemove": observedEdges(views[1]), "afterExplicitRetype": observedEdges(views[2])}
	case "B25":
		if len(views) != 4 {
			t.Fatalf("B25 observed %d detail reads", len(views))
		}
		states := []map[string]any{}
		for i, operation := range []string{"title", "priority", "description"} {
			states = append(states, map[string]any{"operation": operation, "relations": observedRelations(t, views[i+1])})
		}
		last := views[3]
		observed = map[string]any{"before": observedRelations(t, views[0]), "afterEachUpdate": states,
			"finalFields": map[string]any{"title": last.Fields["title"], "priority": last.Fields["priority"], "description": last.Fields["description"]}}
	default:
		t.Fatalf("unexpected recorded card %s", vector.Card)
	}
	data, err := json.Marshal(observed)
	if err != nil {
		t.Fatal(err)
	}
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		name := strings.ReplaceAll(t.Name(), "/", "_") + ".observed.json"
		if err := os.WriteFile(filepath.Join(dir, name), append(append([]byte(nil), data...), '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := acceptance.EqualJSON(vector.Observations, data); err != nil {
		t.Fatalf("%s real memory replay differs from independent reference: %v", vector.Card, err)
	}
	t.Logf("%s real memory replay matches independent reference observations: %s", vector.Card, data)
	return f.trace.Frames(), data
}

func TestRecordedReferenceSlicesReplayThroughExistingHarness(t *testing.T) {
	vectors := recordedVectors(t)
	for _, card := range []string{"B07", "B25"} {
		t.Run(card, func(t *testing.T) {
			vector, ok := vectors[card]
			if !ok {
				t.Fatalf("missing recorded %s vector", card)
			}
			var first, repeat []acceptance.Frame
			t.Run("first", func(t *testing.T) { first, _ = replayRecordedVector(t, vector) })
			t.Run("repeat", func(t *testing.T) { repeat, _ = replayRecordedVector(t, vector) })
			if !reflect.DeepEqual(first, repeat) {
				t.Fatal("fresh real stacks changed their request/response bytes")
			}
		})
	}
}
