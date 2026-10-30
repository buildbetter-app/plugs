package cardb28

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

//go:embed record_reference.py reference/*.json
var retained embed.FS

const pinnedCommit = "9d6fff760b7a314b476539198504d77ce0959656"

type referenceRecording struct {
	Schema       string `json:"schema"`
	Recording    string `json:"recording"`
	SourceCommit string `json:"sourceCommit"`
	SourceFiles  []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"sourceFiles"`
	SourceTests    []string `json:"sourceTests"`
	BinarySHA256   string   `json:"binarySha256"`
	RecorderSHA256 string   `json:"recorderSha256"`
	Commands       []struct {
		Argv     []string `json:"argv"`
		ExitCode int      `json:"exitCode"`
	} `json:"commands"`
	Observations struct {
		Before        int    `json:"callbacksBeforeGuardedRefusal"`
		After         int    `json:"callbacksAfterGuardedRefusal"`
		ProjectionSHA string `json:"projectionSha256"`
	} `json:"observations"`
	Projection []struct {
		Sequence int    `json:"sequence"`
		Event    string `json:"event"`
		IssueID  string `json:"issueId"`
	} `json:"projection"`
	Limits []string `json:"limits"`
}

type provenance struct {
	Schema string `json:"schema"`
	Source struct {
		Commit string `json:"commit"`
		Tree   string `json:"tree"`
		Files  []struct {
			Path, GitBlob, SHA256 string
		} `json:"files"`
	} `json:"source"`
	BinarySHA256 string `json:"binarySha256"`
	Recorder     struct {
		Path, SHA256 string
	} `json:"recorder"`
	Recordings []struct {
		Path, FileSHA256, ProjectionSHA256 string
	} `json:"recordings"`
	RetainedSelection string `json:"retainedSelection"`
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func loadRecording(t *testing.T, path string) ([]byte, referenceRecording) {
	t.Helper()
	raw, err := retained.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value referenceRecording
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return raw, value
}

func TestB28FreshReferenceRecordingsRetainStablePostCommitFacts(t *testing.T) {
	provenanceRaw, err := retained.ReadFile("reference/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof provenance
	if err := json.Unmarshal(provenanceRaw, &proof); err != nil {
		t.Fatal(err)
	}
	recorder, err := retained.ReadFile("record_reference.py")
	if err != nil {
		t.Fatal(err)
	}
	if proof.Schema != "plug.cardb28.reference-provenance.v1" || proof.Source.Commit != pinnedCommit || proof.Source.Tree == "" || len(proof.Source.Files) != 6 || proof.BinarySHA256 == "" || proof.Recorder.Path != "spec/cardb28/record_reference.py" || proof.Recorder.SHA256 != sha(recorder) || proof.RetainedSelection == "" {
		t.Fatalf("incomplete provenance: %+v", proof)
	}

	var twins []referenceRecording
	for _, item := range proof.Recordings {
		raw, recording := loadRecording(t, item.Path)
		if item.FileSHA256 != sha(raw) || item.ProjectionSHA256 != recording.Observations.ProjectionSHA {
			t.Fatalf("retained digest mismatch for %s", item.Path)
		}
		if recording.Schema != "plug.cardb28.reference.v1" || recording.SourceCommit != pinnedCommit || recording.BinarySHA256 != proof.BinarySHA256 || recording.RecorderSHA256 != proof.Recorder.SHA256 || len(recording.SourceFiles) != len(proof.Source.Files) || len(recording.SourceTests) != 5 || len(recording.Commands) != 5 || len(recording.Limits) != 3 {
			t.Fatalf("incomplete recording %s: %+v", item.Path, recording)
		}
		for index, source := range recording.SourceFiles {
			if source.Path != proof.Source.Files[index].Path || source.SHA256 != proof.Source.Files[index].SHA256 || proof.Source.Files[index].GitBlob == "" {
				t.Fatalf("source provenance mismatch at %d: %+v %+v", index, source, proof.Source.Files[index])
			}
		}
		if recording.Observations.Before != 2 || recording.Observations.After != 2 || recording.Commands[3].ExitCode == 0 {
			t.Fatalf("guarded refusal did not suppress callback: %+v", recording)
		}
		twins = append(twins, recording)
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0].Projection, twins[1].Projection) || twins[0].Observations.ProjectionSHA != twins[1].Observations.ProjectionSHA {
		t.Fatalf("independent reference projections diverged: %+v", twins)
	}
	want := []struct {
		Sequence int
		Event    string
		IssueID  string
	}{{1, "create", "b28-1"}, {2, "update", "b28-1"}, {3, "close", "b28-1"}}
	for index, item := range twins[0].Projection {
		if item.Sequence != want[index].Sequence || item.Event != want[index].Event || item.IssueID != want[index].IssueID {
			t.Fatalf("reference order changed at %d: %+v", index, item)
		}
	}
	t.Log("B28 reference: independent pinned runs agree on stable post-commit order and refusal suppression; temporal bytes remain digest-only")
}
