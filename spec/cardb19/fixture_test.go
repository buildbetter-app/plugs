package cardb19_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

//go:embed reference.json testdata/*/*
var referenceFiles embed.FS

type state struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Status      string `json:"status"`
	Priority    int    `json:"priority"`
}

type recordedReference struct {
	SourceCommit string `json:"source_commit"`
	BinarySHA256 string `json:"binary_sha256"`
	Workload     struct {
		InitialTitle string `json:"initial_title"`
		state
	} `json:"workload"`
	Recordings []struct {
		Workspace      string `json:"workspace"`
		ExportSHA256   string `json:"export_sha256"`
		ImportedSHA256 string `json:"imported_sha256"`
		Projection     state  `json:"projection"`
	} `json:"recordings"`
}

func reference(t *testing.T) recordedReference {
	t.Helper()
	data, err := referenceFiles.ReadFile("reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var value recordedReference
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	if value.SourceCommit != "9d6fff760b7a314b476539198504d77ce0959656" || value.BinarySHA256 != "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3" || len(value.Recordings) != 2 {
		t.Fatal("reference source, executable or independent recording count changed")
	}
	if value.Recordings[0].Workspace == value.Recordings[1].Workspace {
		t.Fatal("reference recordings reused a workspace")
	}
	for i, recording := range value.Recordings {
		for _, item := range []struct{ name, digest string }{{"export.jsonl", recording.ExportSHA256}, {"imported.json", recording.ImportedSHA256}} {
			data, err := referenceFiles.ReadFile(fmt.Sprintf("testdata/recording-%d/%s", i+1, item.name))
			if err != nil || sha(data) != item.digest {
				t.Fatalf("retained reference bytes changed: %s: %v", item.name, err)
			}
			var actual state
			if item.name == "export.jsonl" {
				decoder := json.NewDecoder(bytes.NewReader(data))
				if err := decoder.Decode(&actual); err != nil {
					t.Fatal(err)
				}
				if err := decoder.Decode(new(any)); err != io.EOF {
					t.Fatal("reference export is not exactly one record")
				}
			} else {
				var rows []state
				if err := json.Unmarshal(data, &rows); err != nil || len(rows) != 1 {
					t.Fatalf("reference import read: %v", err)
				}
				actual = rows[0]
			}
			if actual != recording.Projection || actual != value.Recordings[0].Projection {
				t.Fatalf("independent reference state differs: %+v", actual)
			}
		}
	}
	return value
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func adoptedState(t *testing.T) state {
	t.Helper()
	want := reference(t).Recordings[0].Projection
	if want.Status != "open" {
		t.Fatalf("recorded status has no adopted mapping in this slice: %q", want.Status)
	}
	// B03/A3 use ontology slots; the admitted default's unstarted key is todo.
	// Keep the independently recorded source bytes and state unchanged.
	want.Status = "todo"
	return want
}

var presence = map[string]any{"mode": "presence_check", "challenge": "cardb19", "proof": "cardb19-local-proof"}

type localStore struct {
	local *cli.Local
	store driver.Store
}

func openLocal(t *testing.T, path string) *localStore {
	t.Helper()
	var storage driver.Store
	sequence := 0
	local, err := cli.OpenLocal(context.Background(), cli.LocalConfig{
		Project: "cardb19", Agent: "cardb19-recorder",
		Storage: cli.StorageConfig{Driver: "sqlite", DSN: path},
		Factory: func(ctx context.Context, config cli.StorageConfig) (driver.Store, func() error, error) {
			var err error
			storage, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
			if err != nil {
				return nil, nil, err
			}
			return storage, storage.Close, nil
		},
		PipelineOptions: []pipeline.Option{
			pipeline.WithClock(func() time.Time { return time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC) }),
			pipeline.WithIDGenerator(func() (string, error) {
				sequence++
				return fmt.Sprintf("pl-%032x", sequence), nil
			}),
		},
		PresenceVerifier: restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, proof restore.Presence) error {
			if method == "" || len(digest) != 64 || proof["challenge"] != presence["challenge"] || proof["proof"] != presence["proof"] {
				return fmt.Errorf("test host requires method, digest and local proof")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Error(err)
		}
	})
	return &localStore{local: local, store: storage}
}

func (s *localStore) close(t *testing.T) {
	t.Helper()
	if err := s.local.Close(); err != nil {
		t.Fatal(err)
	}
}

func call(t *testing.T, s *localStore, method string, params map[string]any, output any) {
	t.Helper()
	result, err := s.local.Client.Call(context.Background(), method, params, "")
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	if err := json.Unmarshal(result.JSON, output); err != nil {
		t.Fatalf("%s result %s: %v", method, result.JSON, err)
	}
	retain(t, method+".json", result.JSON)
}

func seed(t *testing.T, s *localStore) model.Ref {
	t.Helper()
	ref := reference(t)
	var created model.Commit
	call(t, s, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": ref.Workload.InitialTitle, "description": ref.Workload.Description, "priority": ref.Workload.Priority}}, &created)
	if len(created.Results) != 1 || created.Revision.String() != "1" {
		t.Fatalf("create did not publish one base: %+v", created)
	}
	id := created.Results[0].Ref
	var updated model.Commit
	call(t, s, "bead.update", map[string]any{"id": id, "fields": map[string]any{"title": ref.Workload.Title}}, &updated)
	if updated.Revision.String() != "2" {
		t.Fatalf("update revision=%v", updated.Revision)
	}
	return id
}

func metadata(t *testing.T, s *localStore) driver.Meta {
	t.Helper()
	meta, err := s.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	retain(t, "store-meta.json", data)
	return meta
}

func dump(t *testing.T, s *localStore, history string) (string, []byte, *dumpformat.Archive) {
	t.Helper()
	path := filepath.Join(t.TempDir(), history+".tar")
	var output bytes.Buffer
	session := cli.Session{Client: s.local.Client, Out: &output, Err: &output, HumanPresence: presence}
	if err := session.Run(context.Background(), []string{"dump", "--history", history, "--output", path, "--page-size", "512"}); err != nil {
		t.Fatalf("paged admin.dump: %v; %s", err, output.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil || !archive.Native() || archive.Manifest().History != history {
		t.Fatalf("native %s archive validation: %v", history, err)
	}
	retain(t, "native-"+history+".tar", data)
	return path, data, archive
}

func restoreArchive(s *localStore, path, history string) error {
	session := cli.Session{Client: s.local.Client, Out: io.Discard, Err: io.Discard, HumanPresence: presence}
	return session.Run(context.Background(), []string{"restore", path, "--history", history})
}

func assertState(t *testing.T, s *localStore, id model.Ref) {
	t.Helper()
	var actual struct {
		Bead struct {
			Status string
			Fields map[string]any
		}
	}
	call(t, s, "bead.get", map[string]any{"id": id}, &actual)
	data, err := json.Marshal(actual.Bead.Fields)
	if err != nil {
		t.Fatal(err)
	}
	var projected state
	if err := json.Unmarshal(data, &projected); err != nil {
		t.Fatal(err)
	}
	projected.Status = actual.Bead.Status
	want := adoptedState(t)
	if !reflect.DeepEqual(projected, want) {
		t.Fatalf("Plug state=%+v reference state=%+v", projected, want)
	}
}

func retain(t *testing.T, name string, data []byte) {
	t.Helper()
	if directory := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); directory != "" {
		path := filepath.Join(directory, t.Name())
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(path, name)
		for sequence := 2; ; sequence++ {
			_, err := os.Stat(output)
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			output = filepath.Join(path, fmt.Sprintf("%d-%s", sequence, name))
		}
		if err := os.WriteFile(output, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
