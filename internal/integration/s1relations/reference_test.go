package s1relations_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var referencePath = flag.String("reference", "", "immutable original B25 recording runfile")

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

func originalB25(t *testing.T) referenceVector {
	t.Helper()
	if *referencePath == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("Bazel must supply the original B25 recording")
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_SRCDIR"), *referencePath))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != "31f68e58ea444c90c0780b44beec91dc6bcd7255c3f2c859b146d4337c01708d" {
		t.Fatal("original recorder bytes changed without reviewed repinning")
	}
	var manifest struct {
		Recorders map[string]struct {
			RawJSON string `json:"rawJSON"`
			Digest  string `json:"digest"`
		} `json:"recorders"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	recording, ok := manifest.Recorders["B25"]
	if !ok {
		t.Fatal("original B25 recording missing")
	}
	digest = sha256.Sum256([]byte(recording.RawJSON))
	if hex.EncodeToString(digest[:]) != recording.Digest {
		t.Fatal("original B25 vector digest mismatch")
	}
	var vector referenceVector
	if err := json.Unmarshal([]byte(recording.RawJSON), &vector); err != nil || vector.Card != "B25" {
		t.Fatalf("original B25 vector: %v", err)
	}
	return vector
}

func argument(t *testing.T, args []string, name string) string {
	t.Helper()
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	t.Fatalf("recorded command lacks %s: %v", name, args)
	return ""
}

// The stable projection is the original recorder's declared non-temporal
// projection. Public Get supplies scalar fields, labels and comment bodies;
// the stored dependency image is checked independently of that DTO.
func (f *fixture) observedRelations(label, id string) (map[string]any, map[string]any) {
	f.t.Helper()
	got := f.call("bead.get", label, map[string]any{"id": ref(id)})
	if got.Error != nil {
		f.t.Fatalf("public Get refused: %s", got.Error.Data.Code)
	}
	var value struct {
		Bead struct {
			Fields map[string]any `json:"fields"`
		} `json:"bead"`
		Comments []struct {
			Body string `json:"body"`
		} `json:"comments"`
	}
	if err := json.Unmarshal(got.Result, &value); err != nil {
		f.t.Fatal(err)
	}
	doc := f.document(id)
	equalJSON(f.t, doc.Fields, value.Bead.Fields)
	comments := make([]string, len(value.Comments))
	for i, comment := range value.Comments {
		comments[i] = comment.Body
	}
	storedComments := make([]string, len(doc.Comments))
	for i, comment := range doc.Comments {
		storedComments[i] = comment.Body
	}
	equalJSON(f.t, storedComments, comments)
	if value.Bead.Fields["labels"] == nil {
		f.t.Fatal("declared label field disappeared from public Get")
	}
	edges := make([]map[string]string, len(doc.Links))
	for i, edge := range doc.Links {
		edges[i] = map[string]string{"from": id, "to": edge.To.ID, "kind": edge.Kind}
	}
	return map[string]any{"labels": value.Bead.Fields["labels"], "dependencies": edges,
		"comments": comments, "commentCount": len(comments)}, value.Bead.Fields
}

func replayOriginal(t *testing.T, kind string, vector referenceVector) any {
	t.Helper()
	f := newFixture(t, kind, vector.Inputs.IDs.Explicit)
	views := []map[string]any{}
	var fields map[string]any
	for step, command := range vector.Commands {
		args := command.Args
		if command.Exit != 0 || len(args) == 0 {
			t.Fatalf("original command %d did not succeed", step)
		}
		label := fmt.Sprintf("reference-%d", step)
		switch args[0] {
		case "init":
			// Each iteration already owns a fresh real Plug store.
		case "create":
			created := f.write("bead.create", label, map[string]any{"type": "issue", "fields": map[string]any{
				"title": argument(t, args, "--title"), "priority": json.Number(argument(t, args, "--priority")),
			}})
			if len(created.Results) != 1 || created.Results[0].Ref.ID != argument(t, args, "--id") {
				t.Fatalf("recorded identity was not minted: %+v", created)
			}
		case "dep":
			if args[1] != "add" {
				t.Fatalf("unsupported original dependency command: %v", args)
			}
			f.write("bead.link", label, map[string]any{"from": ref(args[2]), "to": ref(args[3]), "kind": argument(t, args, "--type")})
		case "label":
			if args[1] != "add" {
				t.Fatalf("unsupported original label command: %v", args)
			}
			var labels []string
			if value := f.document(args[2]).Fields["labels"]; value != nil {
				if err := json.Unmarshal(jsonBytes(t, value), &labels); err != nil {
					t.Fatal(err)
				}
			}
			labels = append(labels, args[3])
			f.write("bead.update", label, map[string]any{"id": ref(args[2]), "fields": map[string]any{"labels": labels}})
		case "comments":
			if args[1] != "add" {
				t.Fatalf("unsupported original comment command: %v", args)
			}
			f.write("bead.comment", label, map[string]any{"id": ref(args[2]), "body": args[3]})
		case "update":
			field, value := strings.TrimPrefix(args[2], "--"), any(args[3])
			if field == "priority" {
				value = json.Number(args[3])
			}
			f.write("bead.update", label, map[string]any{"id": ref(args[1]), "fields": map[string]any{field: value}})
		case "show":
			var relations map[string]any
			relations, fields = f.observedRelations(label, args[1])
			views = append(views, relations)
		default:
			t.Fatalf("unsupported original command: %v", args)
		}
	}
	if len(views) != 4 {
		t.Fatalf("original B25 must exercise four distinct relation reads, got %d", len(views))
	}
	states := make([]map[string]any, 3)
	for i, field := range []string{"title", "priority", "description"} {
		states[i] = map[string]any{"operation": field, "relations": views[i+1]}
	}
	observed := map[string]any{"before": views[0], "afterEachUpdate": states,
		"finalFields": map[string]any{"title": fields["title"], "priority": fields["priority"], "description": fields["description"]}}
	var expected any
	if err := json.Unmarshal(vector.Observations, &expected); err != nil {
		t.Fatal(err)
	}
	equalJSON(t, expected, observed)
	before := f.image()
	f.reopen()
	if string(before) != string(f.image()) {
		t.Fatal("server/store reopen changed the recorded durable graph/history/event image")
	}
	reopened, reopenedFields := f.observedRelations("reopened", vector.Inputs.IDs.Explicit[0])
	equalJSON(t, views[3], reopened)
	equalJSON(t, fields, reopenedFields)
	t.Logf("immutable original B25 projection on %s: %s", kind, jsonBytes(t, observed))
	return observed
}

func TestB25OriginalDeclaredLabelRecordingReplaysAcrossAllStores(t *testing.T) {
	vector := originalB25(t)
	stores(t, func(t *testing.T, kind string) {
		var first any
		t.Run("first", func(t *testing.T) { first = replayOriginal(t, kind, vector) })
		t.Run("repeat", func(t *testing.T) { equalJSON(t, first, replayOriginal(t, kind, vector)) })
	})
}
